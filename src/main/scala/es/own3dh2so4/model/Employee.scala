package es.own3dh2so4.model

/**
  * Created by david on 3/04/17.
  */
case class Employee(EmployeeID : String,
                    LastName : String, FirstName : String, Title : String,
                    BirthDate : String, HireDate : String,
                    City : String, State : String, Zip : String,  Country : String,
                    ReportsTo : String)

case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
                 OrderDate : String, ShipCountry : String)

case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Double,
                        Qty : Int, Discount : Double)
