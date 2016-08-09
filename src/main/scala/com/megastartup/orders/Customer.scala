package com.megastartup.orders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

case class Customer (
  CustID: String,
  RegistrationDate: String,
  Region: Int
) {

  def toRow = {
    Row(CustID, RegistrationDate, Region)
  }

  def mkString(sep: String): String = {
    s"%s$sep%s$sep%d".format(CustID, RegistrationDate, Region)
  }
}

object Customer {

  // converts Staging to Customer
  def fromStaging (src: RDD[Staging]): RDD[Customer] = {
    src.map(c => Customer(c.CustID, c.RegistrationDate, c.Region))
  }

  // converts storage data to Customer
  def fromStorage (src: RDD[String]): RDD[Customer] = {
    src.map(_.split(","))
      .map(r => Customer( r(0).trim,
                          r(1).trim,
                          r(2).trim.toInt ))
  }

  // schema for DataFrame
  val schema =
    StructType(
      StructField("CustID",           StringType,  false) ::
      StructField("RegistrationDate", StringType,  true) ::
      StructField("Region",           IntegerType, true) ::
      Nil)

}

class CustomerMerger(sqlc: SQLContext) {

  // merge takes existing customers and customers from current loading and
  // returns new RDD with all customers and delta that was added
  def merge (existing: RDD[Customer], current: RDD[Customer]): (RDD[Customer], RDD[Customer]) = {

    val existingDF = sqlc.createDataFrame(existing.map(_.toRow), Customer.schema)
    existingDF.registerTempTable("existing")

    val currentDF = sqlc.createDataFrame(current.map(_.toRow), Customer.schema)
    currentDF.registerTempTable("current")

    val sql = """
     |SELECT o.CustID
     |     , o.RegistrationDate
     |     , o.Region
     |  FROM current o
     |  LEFT JOIN existing c ON c.CustID = o.CustID
     | WHERE c.CustID IS NULL
     |""".stripMargin

    val delta = sqlc.sql(sql).map(row => Customer(
      row(0).toString, row(1).toString, row(2).toString.toInt))

    (existing.union(delta), delta)
  }

}



