package es.own3dh2so4.ch11

import es.own3dh2so4.Properties
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
  * Created by david on 9/04/17.
  */
object SparkClassification extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "titanic/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val passengers = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles)
  println(s"Total passengers read ${passengers.count()}")
  passengers.printSchema()
  passengers.show(5)

  val passengers1 = passengers.select(passengers("Pclass"),passengers("Survived").cast(DoubleType).as("Survived"),
          passengers("Gender"),passengers("Age"),passengers("SibSp"),passengers("Parch"),passengers("Fare"))
  passengers1.show()

  val indexer =  new StringIndexer()
  indexer.setInputCol("Gender")
  indexer.setOutputCol("GenderCat")
  val passengers2 = indexer.fit(passengers1).transform(passengers1)
  passengers2.show(5)

  val passengers3 = passengers2.na.drop()
  println(s" Original = ${passengers2.count()}. Final = ${passengers3.count()} . Difference = ${ passengers2.count() - passengers3.count()}" )

  val assembler = new VectorAssembler()
  assembler.setInputCols(Array("Pclass","GenderCat","Age","SibSp","Parch","Fare"))
  assembler.setOutputCol("features")
  val passengers4 = assembler.transform(passengers3)
  passengers4.show(5)

  val Array(train,test) = passengers4.randomSplit(Array(0.9,0.1))
  println(s"Train = ${train.count()} Test = ${test.count()}")

  val algTree = new DecisionTreeClassifier()
  algTree.setLabelCol("Survived")
  algTree.setImpurity("gini")
  algTree.setMaxBins(32)
  algTree.setMaxDepth(5)

  val mdlTree = algTree.fit(train)
  println("The tree has %d nodes.".format(mdlTree.numNodes))
  println(mdlTree.toDebugString)
  println(mdlTree.toString)
  println(mdlTree.featureImportances)

  val predictions = mdlTree.transform(test)
  predictions.show(5)

  val evaluator = new MulticlassClassificationEvaluator()
  evaluator.setLabelCol("Survived")
  evaluator.setMetricName("accuracy")
  val acuracy = evaluator.evaluate(predictions)
  println("Test Accuracy = %.2f%%".format(acuracy*100))


}
