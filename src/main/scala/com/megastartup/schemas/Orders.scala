package com.megastartup.schemas

import org.apache.spark.sql.types._

object Orders {
  val orders =
    StructType(
      StructField("CustID",     StringType,  false) ::
      StructField("OrdersCnt",  IntegerType, false) ::
      StructField("PaymentSum", DoubleType,  true) ::
      Nil)
}
