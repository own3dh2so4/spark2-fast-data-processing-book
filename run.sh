#!/usr/bin/env bash

echo "[INFO] Compile:"
sbt assembly
echo "[INFO] Execute:"
spark-submit --class "es.own3dh2so4.ch${1}.Spark${2}" target/scala-2.11/spark2-fast-data-processing-book-assembly-1.0.jar