package com.megastartup.orders

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import com.megastartup.lov.Regions
import com.megastartup.aux.Options.toOptionDouble
import com.megastartup.schemas.{Customer, Orders}


case class OrderStg(
  CustID: String,
  Region: Int,
  RegistrationDate: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double]
)

object OrderStg {

  def fromEighty (src: RDD[String]): RDD[OrderStg] = {
    src.map(_.split(","))
      .map(p => OrderStg( p(0).trim+p(1).trim,          // branch+client_id
                          Regions.ByName(p(2).trim),    // lookup region id by name
                          p(3).trim,                    // TODO: add date parsing
                          p(4).trim.toInt,
                          toOptionDouble(p(5)) ))
  }

  def fromMinodo (src: RDD[String]): RDD[OrderStg] = {

    val regionIndex = (i: String) => {
      val name = i match {
        case "1" => "Москва"
        case "2" => "Санкт-Петербург"
        case  _  => "N/A"
      }
      Regions.ByName(name)
    }

    src.map(_.split(","))
      .map(p => OrderStg( p(0).trim,                    // email
                          regionIndex(p(1).trim),       // lookup region id by index
                          p(2).trim,                    // TODO: add convertioni from timestamp to date
                          p(3).trim.toInt,
                          toOptionDouble(p(4)) ))
  }
}


case class CustomerDim(
  CustID: String,
  RegistrationDate: String,
  Region: Int
)


class CustomerDimMerger(sqlc: SQLContext) {

  // merge takes existing customers and customers from current loading and
  // returns new RDD with all customers and delta that was added
  def merge (existing: RDD[CustomerDim], current: RDD[CustomerDim]): (RDD[CustomerDim], RDD[CustomerDim]) = {

    val toRow = (c: CustomerDim) => Row(c.CustID, c.RegistrationDate, c.Region)

    val existingDF = sqlc.createDataFrame(existing.map(toRow), Customer.schema)
    existingDF.registerTempTable("existing")

    val currentDF = sqlc.createDataFrame(current.map(toRow), Customer.schema)
    currentDF.registerTempTable("current")

    val sql = """
     |SELECT o.CustID
     |     , o.RegistrationDate
     |     , o.Region
     |  FROM current o
     |  LEFT JOIN existing c ON c.CustID = o.CustID
     | WHERE c.CustID IS NULL
     |""".stripMargin

    val delta = sqlc.sql(sql).map(row => CustomerDim(
      row(0).toString, row(1).toString, row(2).toString.toInt))

    (existing.union(delta), delta)
  }

}

object CustomerDim {

  def fromStg (src: RDD[OrderStg]): RDD[CustomerDim] = {
    src.map(c => CustomerDim(c.CustID, c.RegistrationDate, c.Region))
  }

  def fromText (src: RDD[String]): RDD[CustomerDim] = {
    src.map(_.split(","))
      .map(r => CustomerDim( r(0).trim,
                             r(1).trim,
                             r(2).trim.toInt ))
  }

  def mkString(c: CustomerDim, sep: String): String = {
    s"%s$sep%s$sep%d".format(c.CustID, c.RegistrationDate, c.Region)
  }
}


case class OrderFact(
  CustID: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double]
)

object OrderFact {

  def orders (src: RDD[OrderStg]): RDD[OrderFact] = {
    src.map(f => OrderFact(f.CustID, f.OrdersCnt, f.PaymentSum))
  }

  def mkString(o: OrderFact, sep: String): String = {
    s"%s$sep%d$sep%f".format(o.CustID, o.OrdersCnt, o.PaymentSum)
  }
}


/*
  case class OrderDM(
    CustID: String,
    OrdersCnt: Int,
    PaymentSum: Option[Double],
    DaysOld: Int,
    Region: Int
  )
*/

class OrderDM(sqlc: SQLContext) {

  // orders returns output for DataMart, requires two tables registered within
  // SQLContext - customer (dim) and orders (fact - for current period only)
  def orders(period: String): RDD[String] =
    sqlc
      .sql("""
      |SELECT c.CustID
      |     , COALESCE(o.OrdersCnt, 0) OrdersCnt
      |     , COALESCE(o.PaymentAmt, 0) PaymentAmt
      |     , datediff(s"$period-01", c.RegistrationDate) DaysOld
      |     , c.Region
      |  FROM customer c
      |  LEFT JOIN orders o ON c.CustID = o.CustID
      |""".stripMargin)
      .rdd.map(_.mkString(","))
}


object OrdersETL {

  def main(args: Array[String]) {

    if ( args.length < 3 ) {
      System.err.println("Usage: orders <client-code> <file> <data-dir> <period>")
      System.exit(1)
    }

    val clientCode = args(0).trim
    val fileName   = args(1).trim
    val dataDir    = args(2).trim
    val period     = args(3).trim // reporting period, YYYY-MM

    val appName = "OrdersETL"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    val sc         = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val raw = sc.textFile(fileName)
      .zipWithIndex().filter(_._2 > 0).map(_._1) // removing header row from data

    val stg = clientCode match {
      case "eighty" => OrderStg.fromEighty(raw)
      case "minodo" => OrderStg.fromMinodo(raw)
    }

    val existingCustomers = CustomerDim.fromText(
      sc.textFile(s"$dataDir/dim/customer/$clientCode/*"))

    val currentCustomers = CustomerDim.fromStg(stg)

    val merger = new CustomerDimMerger(sqlContext)

    val (customers, delta) = merger.merge(existingCustomers, currentCustomers)

    // saving delta
    delta.map(CustomerDim.mkString(_, ",")).saveAsTextFile(
      s"$dataDir/dim/customer/$clientCode/$period")

    val customerToRow = (c: CustomerDim) => Row(c.CustID, c.RegistrationDate, c.Region)

    val customersDF = sqlContext.createDataFrame(customers.map(customerToRow), Customer.schema)
    customersDF.registerTempTable("customer")

    customersDF.show()

    val orders = OrderFact.orders(stg)

    // saving orders for current period
    orders.map(OrderFact.mkString(_, ",")).saveAsTextFile(s"$dataDir/fact/orders/$clientCode/$period")

    val ordersToRow = (o: OrderFact) => Row(o.CustID, o.OrdersCnt, o.PaymentSum)
    val ordersDF = sqlContext.createDataFrame(orders.map(ordersToRow), Orders.schema)
    ordersDF.registerTempTable("orders")

    ordersDF.show()

    val dm = new OrderDM(sqlContext).orders(period)
    dm.saveAsTextFile(s"$dataDir/fact/orders/$clientCode/$period")
  }
}
