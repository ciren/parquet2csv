{ stdenv, pkgs, src, buildInputs }:

stdenv.mkDerivation {
  inherit src buildInputs;
  name = "parquet2csv";
  buildPhase = "touch $out";
  installPhase = ":";
}
