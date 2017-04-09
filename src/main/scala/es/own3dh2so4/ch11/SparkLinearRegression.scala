package es.own3dh2so4.ch11

import es.own3dh2so4.Properties
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

/**
  * Created by david on 8/04/17.
  */
object SparkLinearRegression extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "cars/car-milage.csv"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val cars = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles)
  cars.printSchema()
  cars.show(5)

  cars.describe("mpg","hp","weight","automatic").show()

  val cor = cars.stat.corr("hp","weight")
  println("hp to weigth : Correlation = %2.4f".format(cor))
  val cov = cars.stat.cov("hp","weight")
  println("hp to weigth : Covstiance = %2.4f".format(cov))

  val cars1 = cars.na.drop()

  val assembler = new VectorAssembler()
  assembler.setInputCols(Array("displacement","hp","torque","CRatio","RARatio","CarbBarrells","NoOfSpeed","length",
            "width","weight","automatic"))
  assembler.setOutputCol("features")
  val cars2 = assembler.transform(cars1)
  cars2.show(30)

  val train = cars2.filter( cars2("weight") <= 4000)
  val test = cars2.filter( cars2("weight") > 4000)
  test.show()

  println(s"Train = ${train.count()} Test = ${test.count()}")

  val algLR = new LinearRegression()
  algLR.setMaxIter(100)
  algLR.setRegParam(0.3)
  algLR.setElasticNetParam(0.8)
  algLR.setLabelCol("mpg")
  val mdlLR = algLR.fit(train)
  println(s"Coefficents: ${mdlLR.coefficients} Intercept: ${mdlLR.intercept}")

  val trSummary = mdlLR.summary
  println(s"numIterations: ${trSummary.totalIterations}")
  println(s"Iterarion Summary History: ${trSummary.objectiveHistory.toList}")
  trSummary.residuals.show()
  println(s"RMSE: ${trSummary.rootMeanSquaredError}")
  println(s"r2: ${trSummary.r2}")

  val predictions = mdlLR.transform(test)
  predictions.show()

  val evaluator = new RegressionEvaluator()
  evaluator.setLabelCol("mpg")
  val rmse = evaluator.evaluate(predictions)
  println("Root Mean Squared Error = " + "%6.3f".format(rmse))

  evaluator.setMetricName("mse")
  val mse = evaluator.evaluate(predictions)
  println("Mean Squared Error = " + "%6.3f".format(mse))
}
