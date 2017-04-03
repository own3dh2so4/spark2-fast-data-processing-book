package es.own3dh2so4.ch9

import es.own3dh2so4.Properties
import org.apache.spark.sql.functions.{avg, mean}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by david on 2/04/17.
  */
object SparkDataSetCars extends App {

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "cars/"
  val outputFiles = prop("output.folder").getOrElse("") + "cars/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val cars = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles + "cars.csv")
  println(s"Cars count ${cars.count()}")
  cars.show(5)
  cars.printSchema()

  //save
  cars.write.mode(SaveMode.Overwrite).option("header","true").csv(outputFiles+"/csv")
  cars.write.mode(SaveMode.Overwrite).partitionBy("year").parquet(outputFiles+"/parquet")

  println("Data details")
  val carsM = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles + "car-milage.csv")
  carsM.describe("mpg","hp","weight","automatic").show()
  carsM.groupBy("automatic").avg("mpg","torque").show()
  carsM.groupBy().avg("mpg","torque").show()
  carsM.agg(avg(carsM("mpg")), mean(carsM("torque")) ).show()
  carsM.groupBy("automatic").avg("mpg","torque","hp","weight").show()

  println("Statical functions")
  val cor = carsM.stat.corr("hp","weight")
  println("hp to weight: Correlation = %.4f".format(cor))
  val cov = carsM.stat.cov("hp","weight")
  println("hp to weight: Covariance = %.4f".format(cov))
  carsM.stat.crosstab("automatic","NoOfSpeed").show()
}
