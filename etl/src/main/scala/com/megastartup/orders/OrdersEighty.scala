package com.megastartup.orders

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//import org.apache.hadoop.mapred.InvalidInputException

import com.megastartup.lovs.Regions
import com.megastartup.schemas.Schemas

object OrdersEighty {

  def main(args: Array[String]) {

    if ( args.length < 3 ) {
      System.err.println("Usage: orders <file> <data-dir> <period>")
      System.exit(1)
    }

    val clientCode = "eighty"
    val fileName   = args(0).trim
    val dataDir    = args(1).trim
    val period     = args(2).trim

    val appName = "Orders-Eighty"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    val sc         = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // specify schema for raw source Data
    val rawSchema =
      StructType(
        StructField("branch",            StringType,  false) ::
        StructField("client_id",         StringType,  false) ::
        StructField("region",            IntegerType, false) ::
        StructField("first_purchase",    StringType,  false) ::
        StructField("orders_count",      IntegerType, false) ::
        StructField("payment_sum",       DoubleType,  true)  ::
        Nil)

    // reading source as RDD
    val rawRDD = sc.textFile(fileName)
      .zipWithIndex().filter(_._2 > 0).map(_._1) // removing header row from data
      .map(_.split(","))
      .map(p => Row( p(0).trim,
                     p(1).trim,
                     Regions.ByName(p(2).trim),
                     p(3).trim,
                     p(4).trim.toInt,
                     p(5).trim.toDouble ))

    // creating DataFrame for source
    val rawDF = sqlContext.createDataFrame(rawRDD, rawSchema)
    rawDF.registerTempTable("raw")

    // cleaning up and normalizing data
    val sourceDF = sqlContext.sql("""
      SELECT CONCAT(branch,client_id) cust_id
           , UNIX_TIMESTAMP(CONCAT(first_purchase, ' 03:00 UTC'), 'yyyy-dd-MM HH:mm z') reg_ts
           , region
           , orders_count orders_cnt
           , payment_sum payment_amt
        FROM raw""")
    sourceDF.registerTempTable("source")

    sourceDF.show()

    val readCustomers = () => {
      val customersRDD = sc.textFile(s"$dataDir/dim/customer/$clientCode/*")
        .map(_.split(","))
        .map(p => Row( p(0).trim,
                       p(1).trim.toLong,
                       p(2).trim.toInt ))

      // creating DataFrame
      sqlContext.createDataFrame(customersRDD, Schemas.CustomerSchema)
    }

    // creating DataFrame for existing customers
    val customersDF = readCustomers()
    customersDF.registerTempTable("customers")

    // picking up new customers and saving them
    val newCustomersDF = sqlContext.sql("""
      SELECT s.cust_id
           , s.reg_ts
           , s.region
        FROM source s
        LEFT JOIN customers c ON c.cust_id = s.cust_id
       WHERE c.cust_id IS NULL""")
    newCustomersDF.registerTempTable("new_customers")

    newCustomersDF.rdd.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dim/customer/$clientCode/$period")

    println("new customers count: " + newCustomersDF.count()) // TODO: remove this line

    // saving orders
    val ordersDF = sqlContext.sql("""
      SELECT cust_id
           , orders_cnt
           , payment_amt
        FROM source s
    """)
    ordersDF.registerTempTable("orders")

    ordersDF.rdd.map(_.mkString(",")).saveAsTextFile(s"$dataDir/fact/orders/$clientCode/$period")

    println("orders count: " + ordersDF.count()) // TODO: remove this line

    // creating output data
    //val allCustomersDF = readCustomers()
    val outputDF = sqlContext.sql("""
        SELECT c.cust_id
             , o.orders_cnt
             , o.payment_amt
             , trunc((current_timestamp() - c.reg_ts) / 60 / 60 / 24) days_old
             , c.region
          FROM (SELECT cust_id, reg_ts, region FROM customers
                UNION ALL
                SELECT cust_id, reg_ts, region FROM new_customers) c
          LEFT JOIN orders o ON c.cust_id = o.cust_id
    """)

    outputDF.rdd.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dm/orders/$clientCode/$period")

  }

}
