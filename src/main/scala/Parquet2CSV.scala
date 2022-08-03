import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import zio.ZIOAppDefault
import zio.ZIOAppArgs
import zio.cli.{Args, CliApp, Command, Exists, HelpDoc, Options}
import zio.cli.HelpDoc.Span.text
import zio.{Scope, ZIO, ZIOAppArgs}
import java.nio.file.Path
import zio.Console.printLine
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType

object Parquet2CSV extends ZIOAppDefault {

  sealed trait Cmd
  object Cmd {
    final case class Convert(fromParquet: Path, toCsv: Path) extends Cmd
  }

  val parquetFileArg = Args.file("parquet source file")
  val csvFileArg = Args.file("csv target file")
  val convert = Command("convert", Options.none, parquetFileArg ++ csvFileArg)

  val command: Command[Cmd] =
    Command("parquet2csv", Options.none, Args.none).subcommands(convert)
      .map { case (from, to) => Cmd.Convert(from, to) }

  val parquet2csvApp = CliApp.make(
    name = "parquet2csv",
    version = "1.0.0",
    summary = text("Conversion of Parquet files into CSV format"),
    command = command
  ) {
    case Cmd.Convert(fromParquet, toCsv) =>
      for {
        _ <- printLine(s"Executing `convert $fromParquet $toCsv")
        _ <- ZIO.scoped {
          ZIO.acquireReleaseWith(createSparkContext)(x => ZIO.succeed(x.close())) { spark =>
            ZIO.attemptBlockingIO {
              // read parquet file
              val df = spark.read.format("parquet").load(fromParquet.toString())

              // df.show()
              // df.printSchema()

              val df2 = flattenDataFrame(df)
              //df2.printSchema()

              // convert to csv
              df2.coalesce(1)
                .write.mode(SaveMode.Overwrite)
                .option("mode", "append")
                .option("header", "true")
                .csv(toCsv.toString())
            }
          }
        }
        _ <- printLine(s"Conversion complete")
      } yield ()
  }


  def createSparkContext =
    ZIO.attemptBlockingIO {
      val spark = SparkSession.builder().appName("Parquet2CSV").master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      spark
    }

  def flattenDataFrame(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case _: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(
            s"explode_outer($fieldName) as $fieldName"
          )
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataFrame(explodedDf)
        case structType: StructType =>
          val childFieldNames =
            structType.fieldNames.map(childname => fieldName + "." + childname)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
          import org.apache.spark.sql.functions.col

          val renamedCols =
            newFieldNames.map { x =>
              col(x.toString).as(x.toString.replace(".", "_"))
            }

          val explodedDf = df.select(renamedCols: _*)
          return flattenDataFrame(explodedDf)
      case _ =>
      }
    }

    df
  }

  override def run =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- parquet2csvApp.run(args.toList)
    } yield ()
}
