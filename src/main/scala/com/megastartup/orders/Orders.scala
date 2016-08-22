package com.megastartup.orders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
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
    val paymentSum = PaymentSum match {
      case Some(payment) => payment
      case None          => 0
    }
    s"%s$sep%d$sep%.2f".format(CustID, OrdersCnt, paymentSum)
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

