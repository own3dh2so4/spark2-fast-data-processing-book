package es.own3dh2so4.ch11

import es.own3dh2so4.Properties
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Created by david on 9/04/17.
  */
object SparkClustering extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "clustering/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  val data = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles)
  data.printSchema()
  data.show(5)

  val assembler = new VectorAssembler()
  assembler.setInputCols(Array("X","Y"))
  assembler.setOutputCol("features")
  val data1 = assembler.transform(data)
  data1.show(5)

  var algKMeans = new KMeans().setK(2)
  var mdlKMeans = algKMeans.fit(data1)

  var predictions = mdlKMeans.transform(data1)
  predictions.show(3)


  var WSSE = mdlKMeans.computeCost(data1)
  println(s"Within Set Sum of Squared Erros (K=2) = %.3f".format(WSSE))
  println("Cluster Centres (K=2) : " + mdlKMeans.clusterCenters.mkString("<",",",">"))
  println("Cluster Sizes (K = 2) : "+ mdlKMeans.summary.clusterSizes.mkString("<",",",">"))

  algKMeans = new KMeans().setK(4)
  mdlKMeans = algKMeans.fit(data1)

  predictions = mdlKMeans.transform(data1)
  predictions.show(3)


  WSSE = mdlKMeans.computeCost(data1)
  println(s"Within Set Sum of Squared Erros (K = 4) = %.3f".format(WSSE))
  println("Cluster Centres (K = 4) : " + mdlKMeans.clusterCenters.mkString("<",",",">"))
  println("Cluster Sizes (K = 4) : "+ mdlKMeans.summary.clusterSizes.mkString("<",",",">"))



}
