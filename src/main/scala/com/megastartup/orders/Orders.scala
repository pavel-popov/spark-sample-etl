package com.megastartup.orders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._

case class Orders(
  CustID: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double]
) {

  def toRow = {
    Row(CustID, OrdersCnt, PaymentSum)
  }

  // somehow I can't make internal _.mkString thus implementing this custom method,
  // TODO: get rid of this
  def mkString(sep: String): String = {
    s"%s$sep%d$sep%f".format(CustID, OrdersCnt, PaymentSum.getOrElse(0))
  }
}


object Orders {

  def fromStaging (src: RDD[Staging]): RDD[Orders] = {
    src.map(f => Orders(f.CustID, f.OrdersCnt, f.PaymentSum))
  }

  val schema =
    StructType(
      StructField("CustID",     StringType,  false) ::
      StructField("OrdersCnt",  IntegerType, false) ::
      StructField("PaymentSum", DoubleType,  true) ::
      Nil)
}

