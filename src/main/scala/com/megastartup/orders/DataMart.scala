package com.megastartup.orders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class DataMart(sqlc: SQLContext, period: String) {

  def generate(customers: RDD[Customer], orders: RDD[Orders]): RDD[Row] = {

    val customersDF = sqlc.createDataFrame(customers.map(_.toRow), Customer.schema)
    customersDF.registerTempTable("customer")
    customersDF.show()

    val ordersDF = sqlc.createDataFrame(orders.map(_.toRow), Orders.schema)
    ordersDF.registerTempTable("orders")
    ordersDF.show()

    val output = sqlc.sql(s"""
      |SELECT c.CustID
      |     , COALESCE(o.OrdersCnt, 0) OrdersCnt
      |     , COALESCE(o.PaymentSum, 0) PaymentSum
      |     , datediff('$period-01', c.RegistrationDate) DaysOld
      |     , c.Region
      |  FROM customer c
      |  LEFT JOIN orders o ON c.CustID = o.CustID
      |""".stripMargin)
    output.show()

    output.rdd
  }
}
