package es.own3dh2so4.ch9

import es.own3dh2so4.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{hypot, log, log10, sqrt}

/**
  * Created by david on 3/04/17.
  */
object SparkScienctificFunctions extends App{


  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "hypot/"
  val outputFiles = prop("output.folder").getOrElse("") + "hypot/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val aList = List(10,100,1000)
  var aRDD = spark.sparkContext.parallelize(aList)
  import spark.sqlContext.implicits._
  val ds = spark.createDataset(aRDD)
  ds.show()
  ds.printSchema()

  ds.select(ds("value"), log(ds("value")).as("ln")).show()
  ds.select(ds("value"), log10(ds("value"))).as("log10").show()
  ds.select(ds("value"), sqrt(ds("value"))).as("sqrt").show()


  val data = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles)
  println(s"Data has ${data.count()} rows")
  data.show(5)
  data.printSchema()
  data.select(data("X"),data("Y"),hypot(data("X"),data("Y")).as("hypot")).show()
}
