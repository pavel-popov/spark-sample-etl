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

    // quality gate
    val (goodData, badData) = QualityGate.checkItemQuality(raw, clientCode match {
      case "eighty" => QualityGate.eightyItemPass
      case "minodo" => QualityGate.minodoItemPass
    })

    // saving bad data
    badData.saveAsTextFile(s"$dataDir/bad_data/orders/$clientCode/$period")

    // staging
    val stg = clientCode match {
      case "eighty" => Staging.fromEighty(goodData)
      case "minodo" => Staging.fromMinodo(goodData)
    }
    // TODO: refactor code to place all Eighty- and Minodo-specific code to separate Objects

    // stats on staging data
    // TODO: implement

    val existingCustomers = Customer.fromStorage(sc.textFile(s"$dataDir/dim/customer/$clientCode/*"))
    val currentCustomers  = Customer.fromStaging(stg)

    // loading and saving customers delta
    val (customers, delta) = new CustomerMerger(sqlContext).merge(existingCustomers, currentCustomers)
    delta.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dim/customer/$clientCode/$period")

    // loading and saving orders
    val orders = Orders.fromStaging(stg)
    orders.map(_.mkString(",")).saveAsTextFile(s"$dataDir/fact/orders/$clientCode/$period")

    // generating resulting DataMart and saving it
    val dm = new DataMart(sqlContext, period).generate(customers, orders)
    dm.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dm/orders/$clientCode/$period")
  }
}
