package es.own3dh2so4.ch9

import es.own3dh2so4.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc,to_date,month,year}


/**
  * Created by david on 3/04/17.
  */
object SparkDataWrangling extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "nothwindDB/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  println("Read files")
  val employess = spark.read.option("header","true").csv(inputFiles + "NW-Employees.csv")
  employess.printSchema()
  val orders = spark.read.option("header","true").csv(inputFiles + "NW-Orders-01.csv")
  orders.printSchema()
  val orderDetails = spark.read.option("header","true").option("inferSchema","true").
    csv( inputFiles + "NW-Order-Details.csv")
  orderDetails.printSchema()
  println("Read complete")
  println("How many orders were placed by each customer?")
  orders.groupBy("CustomerID").count().sort(desc("count")).show()

  println("How many orders were placed in each country?")
  orders.groupBy("ShipCountry").count().sort(desc("count")).show()

  println("First add total order price")
  val orderDetails1 = orderDetails.select(orderDetails("OrderID"),
    (orderDetails("UnitPrice") * orderDetails("Qty") - (orderDetails("UnitPrice") * orderDetails("Qty") * orderDetails("Discount"))).as("OrderPrice"))
  orderDetails1.show(5)

  println("Now aggregate by order id")
  val orderTot = orderDetails1.groupBy("OrderID").sum("OrderPrice").alias("OrderTotal")
  orderTot.show(5)

  println("Join orderTotal with Orders")
  val orders1 = orders.join(orderTot,orders("OrderID").equalTo(orderTot("OrderID")), "inner").
    select(orders("OrderID"), orders("CustomerID"), orders("OrderDate"), orders("ShipCountry").alias("ShipCountry"),
            orderTot("sum(OrderPrice)").alias("Total"))
  orders1.show(5)

  println("Working with Dates")
  val orders2 = orders1.withColumn("Date", to_date(orders1("OrderDate")))
  orders2.printSchema()
  orders2.show(5)

  println("Adding Month and year columns")
  val order3 = orders2.withColumn("Month",month(orders2("OrderDate"))).withColumn("Year",year(orders2("OrderDate")))
  order3.show(5)

  println("How many orders were placed for each customer, year-wise?")
  val ordersCustomerByMY = order3.groupBy("CustomerID","Year").count().as("TotalOrders")
  ordersCustomerByMY.show(5)

  println("How many order by month/year?")
  val ordersByYM = order3.groupBy("Month","Year").sum("Total").as("Total")
  ordersByYM.show(5)

  println("Average order by customer by year")
  val ordersAvg = order3.groupBy("CustomerID","Year").avg("Total").as("TotalAVGOrders")
  ordersAvg.sort("avg(Total)").show(5)

  println("Average order by customer")
  val ordersCA = order3.groupBy("CustomerID").avg("Total").as("TotalAVGOrders")
  ordersCA.sort(ordersCA("avg(Total)").desc).show(5)

}
