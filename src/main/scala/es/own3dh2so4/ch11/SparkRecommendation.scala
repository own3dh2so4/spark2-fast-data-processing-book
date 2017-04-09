package es.own3dh2so4.ch11

import es.own3dh2so4.Properties
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.functions.split

/**
  * Created by david on 9/04/17.
  */
object SparkRecommendation extends App {

  def parseRating(row:Row) : Rating = {
    val aList = row.getList[String](0)
    Rating(aList.get(0).toInt,aList.get(1).toInt,aList.get(2).toDouble) //.getInt(0), row.getInt(1), row.getDouble(2))
  }

  def rowSqDiff(row:Row) : Double = {
    math.pow( row.getDouble(2) - row.getFloat(3).toDouble,2)
  }

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "medium/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  val startTime = System.nanoTime()

  val movies = spark.read.text(inputFiles + "movies.dat")
  movies.show(5,truncate=false)
  movies.printSchema()
  val ratings = spark.read.text(inputFiles + "ratings.dat")
  ratings.show(5,truncate=false)
  val users = spark.read.text(inputFiles + "users.dat")
  users.show(5,truncate=false)

  println("Got %d ratings from %d users on %d movies.".format(ratings.count(), users.count(), movies.count()))

  val ratings1 = ratings.select(split(ratings("value"),"::")).as("values")
  ratings1.show(5)
  val ratings2 = ratings1.rdd.map( parseRating )
  ratings2.take(3).foreach(println)

  val ratings3 = spark.createDataFrame(ratings2)
  ratings3.show(5)

  val Array(train, test) = ratings3.randomSplit(Array(0.8,0.2))
  println(s"Train = ${train.count()} Test = ${test.count()}")

  val algALS = new ALS()
  algALS.setItemCol("product")
  algALS.setRank(12)
  algALS.setRegParam(0.1)
  algALS.setMaxIter(20)
  val mdlReco = algALS.fit(train)

  val predictions = mdlReco.transform(test)
  predictions.show(5)
  predictions.printSchema()

  val pred = predictions.na.drop()

  println(s"Original = ${predictions.count()} Final = ${pred.count()} Dropped = ${predictions.count() - pred.count()}")

  val evaluator = new RegressionEvaluator()
  evaluator.setLabelCol("rating")
  var rmse = evaluator.evaluate(pred)
  println("Root Mean Squared Error = " +  "%.3f".format(rmse))
  evaluator.setMetricName("mse")
  var mse = evaluator.evaluate(pred)
  println("MEan Squared Error = "+"%.3f".format(mse))
  mse = pred.rdd.map(rowSqDiff).reduce(_+_)
  println("MEan Squared Error (Calculated) = " + "%.3f".format(mse))
  val elapsedTime = (System.nanoTime() - startTime) /1e9
  println("Elapsed time: %.2fseconds".format(elapsedTime))



}
