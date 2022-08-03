import org.apache.spark.sql.{SparkSession, SaveMode}
import zio.ZIOAppDefault
import zio.ZIOAppArgs
import zio.cli.{Args, CliApp, Command, Exists, HelpDoc, Options}
import zio.cli.HelpDoc.Span.text
import zio.{Scope, ZIO, ZIOAppArgs}
import java.nio.file.Path
import zio.Console.printLine

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
      val spark = SparkSession.builder().appName("Parquet2CSV").master("local[*]").getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      //read parquet file
      val df = spark.read.format("parquet").load(fromParquet.toString())

      df.show()
      df.printSchema()

      // TODO: flatten out the data column

      //convert to csv
      df.write.mode(SaveMode.Overwrite).csv(toCsv.toString())

      printLine(s"Executing `convert $fromParquet $toCsv")
  }

  override def run =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- parquet2csvApp.run(args.toList)
    } yield ()
}
