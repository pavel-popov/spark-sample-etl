package com.megastartup.orders

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//import org.apache.hadoop.mapred.InvalidInputException

//import java.text.SimpleDateFormat

import com.megastartup.lov.Regions
import com.megastartup.schemas.Dim

object OrdersEighty {

  def main(args: Array[String]) {

    if ( args.length < 3 ) {
      System.err.println("Usage: orders <file> <data-dir> <period>")
      System.exit(1)
    }

    val clientCode = "eighty"
    val fileName   = args(0).trim
    val dataDir    = args(1).trim
    val period     = args(2).trim // reporting period, YYYY-MM

    val appName = "Orders-Eighty"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    val sc         = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    /*
     * implicit raw data schema:
     *   branch         - string
     *   client_id      - int
     *   region         - string
     *   first_purchase - date YYYY-MM-DD
     *   orders_count   - int
     *   payment_sum    - double
     */

    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    // reading data as RDD and converting it to be suitable for sourceSchema on fly
    val sourceRDD = sc.textFile(fileName)
      .zipWithIndex().filter(_._2 > 0).map(_._1) // removing header row from data
      .map(_.split(","))
      .map(p => Row( p(0).trim+p(1).trim,          // branch+cust_id
                     p(3).trim,                    // TODO: add date parsing
                     Regions.ByName(p(2).trim),    // lookup region id by name
                     p(4).trim.toInt,
                     p(5).trim.toDouble ))

    // specify schema for source Data
    val sourceSchema =
      StructType(
        StructField("cust_id",     StringType,  false) ::
        StructField("reg_dt",      StringType,  false) :: // TODO: change to DateType
        StructField("region",      IntegerType, false) ::
        StructField("orders_cnt",  IntegerType, false) ::
        StructField("payment_amt", DoubleType,  true)  ::
        Nil)

    // creating DataFrame for source
    val sourceDF = sqlContext.createDataFrame(sourceRDD, sourceSchema)
    sourceDF.registerTempTable("source")

    sourceDF.show()

    // readCustomers reads all existing customers from file system into
    // DataFrame.
    val readCustomers = () => {
      val customersRDD = sc.textFile(s"$dataDir/dim/customer/$clientCode/*")
        .map(_.split(","))
        .map(p => Row( p(0).trim,
                       p(1).trim,
                       p(2).trim.toInt ))

      // creating DataFrame
      sqlContext.createDataFrame(customersRDD, Dim.Customer)
    }

    // creating DataFrame for existing customers
    val customersDF = readCustomers()
    customersDF.registerTempTable("existing_customers")

    // picking up new customers and saving them
    val newCustomersDF = sqlContext.sql("""
      SELECT s.cust_id
           , s.reg_dt
           , s.region
        FROM source s
        LEFT JOIN existing_customers c ON c.cust_id = s.cust_id
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
    val outputDF = sqlContext.sql(s"""
        SELECT c.cust_id
             , o.orders_cnt
             , o.payment_amt
             , datediff('$period-01', c.reg_dt) days_old
             , c.region
          FROM (SELECT cust_id, reg_dt, region FROM existing_customers
                UNION ALL
                SELECT cust_id, reg_dt, region FROM new_customers) c
          LEFT JOIN orders o ON c.cust_id = o.cust_id
    """)

    outputDF.rdd.map(_.mkString(",")).saveAsTextFile(s"$dataDir/dm/orders/$clientCode/$period")

  }

}
