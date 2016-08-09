package com.megastartup.schemas

import org.apache.spark.sql.types._

object Orders {
  val schema =
    StructType(
      StructField("CustID",     StringType,  false) ::
      StructField("OrdersCnt",  IntegerType, false) ::
      StructField("PaymentSum", DoubleType,  true) ::
      Nil)
}
