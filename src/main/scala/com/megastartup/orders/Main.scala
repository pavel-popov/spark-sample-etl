package com.megastartup.orders

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main {

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
      case "eighty" => Staging.fromEighty(raw)
      case "minodo" => Staging.fromMinodo(raw)
    }

    val existingCustomers = Customer.fromStorage(sc.textFile(s"$dataDir/dim/customer/$clientCode/*"))

    val currentCustomers = Customer.fromStaging(stg)

    val (customers, delta) = new CustomerMerger(sqlContext).merge(existingCustomers, currentCustomers)

    // saving customers delta for current periods for future reuse
    delta.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dim/customer/$clientCode/$period")

    val customersDF = sqlContext.createDataFrame(customers.map(_.toRow), Customer.schema)
    customersDF.registerTempTable("customer")

    customersDF.show()

    val orders = Orders.fromStaging(stg)

    // saving orders for current period
    orders.map(_.mkString(",")).saveAsTextFile(s"$dataDir/fact/orders/$clientCode/$period")

    val ordersDF = sqlContext.createDataFrame(orders.map(_.toRow), Orders.schema)
    ordersDF.registerTempTable("orders")

    ordersDF.show()

    val output = sqlContext.sql(s"""
      |SELECT c.CustID
      |     , COALESCE(o.OrdersCnt, 0) OrdersCnt
      |     , COALESCE(o.PaymentSum, 0) PaymentSum
      |     , datediff('$period-01', c.RegistrationDate) DaysOld
      |     , c.Region
      |  FROM customer c
      |  LEFT JOIN orders o ON c.CustID = o.CustID
      |""".stripMargin)

    output.show()

    output.rdd.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dm/orders/$clientCode/$period")
  }
}
