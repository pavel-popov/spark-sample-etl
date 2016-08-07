package com.megastartup.schemas

import org.apache.spark.sql.types._

object Schemas {

  val CustomerSchema =
    StructType(
      StructField("cust_id", StringType,  false) ::
      StructField("reg_ts",  LongType,    false) ::
      StructField("region",  IntegerType, false) ::
      Nil)

  val OrdersSchema =
    StructType(
      StructField("cust_id",      StringType,  false) ::
      StructField("orders_cnt",   IntegerType, false) ::
      StructField("payment_amt",  DoubleType,  true) ::
      Nil)
}
