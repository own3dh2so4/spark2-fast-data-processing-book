package es.own3dh2so4

import org.apache.spark.sql.SparkSession

case class Employee(EmployeeID : String,
                    LastName : String, FirstName : String, Title : String,
                    BirthDate : String, HireDate : String,
                    City : String, State : String, Zip : String,  Country : String,
                    ReportsTo : String)

case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
                 OrderDate : String, ShipCountry : String)

case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Double,
                        Qty : Int, Discount : Double)

/**
  * Created by david on 2/04/17.
  */
object SparkSQL extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "nothwindDB/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  println(s"Spark version ${spark.version}")

  import spark.implicits._
  val employess = spark.read.option("header","true").csv(inputFiles + "NW-Employees.csv").as[Employee]

  println(s"Total employees ${employess.count()}")
  employess.show(5)
  employess.explain(true)

  employess.createOrReplaceTempView("EmployeesTable")
  var result = spark.sql("SELECT * FROM EmployeesTable")
  result.show(5)
  result.explain(true)

  result = spark.sql("SELECT * FROM EmployeesTable WHERE State = 'WA'")
  result.show(5)
  result.explain(true)

  val orders = spark.read.option("header","true").csv(inputFiles + "NW-Orders.csv").as[Order]
  println(s"Total orders ${orders.count()}")
  orders.show(5)

  val orderDetails = spark.read.option("header","true").option("inferSchema","true").
    csv( inputFiles + "NW-Order-Details.csv").as[OrderDetails]
  println(s"Total orders details ${orderDetails.count()}")
  orderDetails.show(5)

  orders.createOrReplaceTempView("OrdersTable")
  result =  spark.sql("SELECT * FROM OrdersTable")
  result.show(5)
  result.explain(true)
  orderDetails.createOrReplaceTempView("OrderDetailsTable")
  result = spark.sql("SELECT * FROM OrderDetailsTable")
  result.show(5)
  result.explain(true)

  result = spark.sql("SELECT count(*) as num_orders, e.EmployeeID as employee, CONCAT(e.FirstName,' ', e.LastName) as name " +
    "FROM EmployeesTable e, OrdersTable ot " +
    "WHERE e.EmployeeID = ot.EmployeeID " +
    "GROUP BY e.EmployeeID, e.FirstName, e.LastName " +
    "ORDER BY num_orders DESC")
  result.show(5)
  result.explain(true)

}
