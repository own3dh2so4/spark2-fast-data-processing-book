package es.own3dh2so4.ch9

import es.own3dh2so4.Properties
import org.apache.spark.sql.SparkSession

/**
  * Created by david on 3/04/17.
  */
object SparkTitanic extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "titanic/"
  val outputFiles = prop("output.folder").getOrElse("") + "titanic/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val passengers = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles)
  val passengers1 = passengers.select(passengers("Pclass"),passengers("Survived"),passengers("Gender"),passengers("Age"),
            passengers("SibSp"),passengers("Parch"),passengers("Fare"))
  passengers1.show(5)
  passengers1.printSchema()

  passengers1.groupBy("Gender").count().show()
  passengers1.stat.crosstab("Survived","Gender").show()
  passengers1.stat.crosstab("Survived","SibSp").show()

  val ageDist = passengers1.select(passengers1("Survived"),
    (passengers1("age") - passengers1("age") % 10).cast("int").as("AgeBracket"))

  ageDist.show(3)
  ageDist.stat.crosstab("Survived","AgeBracket").show()

}
