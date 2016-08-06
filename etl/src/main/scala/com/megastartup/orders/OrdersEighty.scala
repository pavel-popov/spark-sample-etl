package org.megastartup.orders

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object OrdersEighty {

  def main(args: Array[String]) {

    if ( args.length < 1 ) {
      System.err.println("Usage: orders <file>")
      System.exit(1)
    }

    val fileName = args(0).trim

    val appName = "Orders-Eighty"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    val sc         = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rows = sc.textFile(fileName)
      .zipWithIndex()
      .filter(_._2 > 0)
      .map(_._1)

    // specify schema
    val schema =
      StructType(
        StructField("branch",            StringType,  false) ::
        StructField("client_id",         IntegerType, false) ::
        StructField("region",            StringType,  false) ::
        StructField("first_purchase",    StringType,  false) :: // TODO: change to DateType
        StructField("orders_count",      IntegerType, false) ::
        StructField("payment_sum",       DoubleType,  false) ::
        Nil)

    val orders = rows.map(_.split(","))
      .map(p => Row( p(0).trim,p(1).trim.toInt,p(2).trim,
                     p(3).trim,p(4).trim.toInt,p(5).trim.toDouble ))

    val ordersDF = sqlContext.createDataFrame(orders, schema)

    ordersDF
      .groupBy("branch")
      .count()
      .show()

    //ordersDF.registerTempTable("order")

    //val resultRDD = sqlContext.sql("SELECT COUNT(*) FROM order")
    //resultRDD.map(t => "Count - " + t(0)).collect().foreach(println)

  }

}
