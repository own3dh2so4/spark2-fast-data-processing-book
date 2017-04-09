package es.own3dh2so4.ch12

import es.own3dh2so4.Properties
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * Created by david on 9/04/17.
  */
case class Person(name:String,age:Int)

object SparkBuildingGraphs extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")


  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()
  val sc = spark.sparkContext

  println(s"Spark version ${spark.version}")



  val defaultPerson = Person("NA",0)

  val vertexList = List(
    (1L, Person("Alice", 18)),
    (2L, Person("Bernie", 17)),
    (3L, Person("Cruz", 15)),
    (4L, Person("Donald", 12)),
    (5L, Person("Ed", 15)),
    (6L, Person("Fran", 10)),
    (7L, Person("Genghis",854))
  )

  val edgeList = List(
    Edge(1L, 2L, 5),
    Edge(1L, 3L, 1),
    Edge(3L, 2L, 5),
    Edge(2L, 4L, 12),
    Edge(4L, 5L, 4),
    Edge(5L, 6L, 2),
    Edge(6L, 7L, 2),
    Edge(7L, 4L, 5),
    Edge(6L, 4L, 4)
  )

  val vertexRDD = sc.parallelize(vertexList)
  val edgeRDD = sc.parallelize(edgeList)
  val graph = Graph(vertexRDD,edgeRDD,defaultPerson)

  println(graph.numEdges)
  println(graph.numVertices)

  val vertices = graph.vertices
  vertices.collect().foreach(println)

  val edge = graph.edges
  edge.collect().foreach(println)

  val tripets = graph.triplets
  tripets.take(3)
  tripets.map( _.toString()).collect().foreach(println)

  val inDeg = graph.inDegrees
  println(inDeg.collect())
  val outDeg = graph.outDegrees
  println(outDeg.collect())
  val allDeg = graph.degrees
  println(allDeg.collect())

  val g1 = graph.subgraph(epred = (edge) => edge.attr > 4)
  g1.triplets.collect().foreach(println)

  val g2 = graph.subgraph(vpred = (id,person) => person.age > 21)
  println("printing g2")
  g2.triplets.collect().foreach(println)
  println(s" Vetext = ${g2.vertices.count()} Edges = ${g2.edges.count()}")

  val cc = graph.connectedComponents()
  cc.triplets.collect().foreach(println)

  println("Vertices")
  graph.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect().foreach(println)
  println(cc.vertices.map(_._2).collect.distinct.length)
  println("List the components and its number of nodes in the descending order")
  cc.vertices.groupBy(_._2).map(p=>(p._1,p._2.size)).sortBy(_._2,ascending = false).collect().foreach(println)


  println("Strongly connected components")
  val ccS = graph.stronglyConnectedComponents(10)
  ccS.triplets.collect().foreach(println)
  ccS.vertices.map(_.swap).groupByKey().map(_._2).collect().foreach(println)
  println("No of connected components")
  println(ccS.vertices.map(_._2).collect().distinct.length)

  println("Count triangles")
  val triCounts = graph.triangleCount()
  println(triCounts)
  val triangleCounts = triCounts.vertices.collect()


  val ranks = graph.pageRank(0.1).vertices
  ranks.collect().foreach(println)
  val topVertices = ranks.sortBy(_._2,ascending = false ).collect().foreach(println)

  println("Older follower")
  val olderFollower = graph.aggregateMessages[Int](
    edgeContext => edgeContext.sendToDst(edgeContext.srcAttr.age), math.max
  )
  olderFollower.collect().foreach(println)

  println("Older followee")
  val olderFollowee = graph.aggregateMessages[Int](
    edgeContext => edgeContext.sendToDst(edgeContext.dstAttr.age), math.max
  )
  olderFollowee.collect().foreach(println)

  println("inDegree with aggregateMessages")
  var iDegree = graph.aggregateMessages[Int](
    ec => ec.sendToDst(1), _+_
  )
  iDegree.collect().foreach(println)

  println("onDegree with aggregateMessages")
  var oDegree = graph.aggregateMessages[Int](
    ec => ec.sendToSrc(1), _+_
  )
  oDegree.collect().foreach(println)


}
