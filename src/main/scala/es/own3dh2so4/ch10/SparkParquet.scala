package es.own3dh2so4.ch10

import es.own3dh2so4.Properties
import org.apache.spark.sql.SparkSession

/**
  * Created by david on 8/04/17.
  */
object SparkParquet extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "nothwindDB/"
  val outputFiles = prop("output.folder").getOrElse("") + "nothwindDB_parquet/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  println("Read files")
  val employess = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles + "NW-Employees.csv")
  println("Saving data in parquet")
  employess.write.mode("overwrite").parquet(outputFiles + "employees")

  println("Now, read the parquet file")
  val employeesParquet = spark.read.parquet(outputFiles + "employees")
  employeesParquet.printSchema()
  employeesParquet.show(10)

}
