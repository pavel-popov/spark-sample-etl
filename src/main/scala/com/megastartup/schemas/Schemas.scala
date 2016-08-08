/**
 * Schema definitions.
 */
package com.megastartup.schemas

import org.apache.spark.sql.types._


// Schemas for Dimensions.
object Dim {

  val Customer =
    StructType(
      StructField("cust_id", StringType,  false) ::
      StructField("reg_dt",  StringType,  false) ::
      StructField("region",  IntegerType, false) ::
      Nil)
}

// Schemas for Facts.
object Fact {

  val Orders =
    StructType(
      StructField("cust_id",      StringType,  false) ::
      StructField("orders_cnt",   IntegerType, false) ::
      StructField("payment_amt",  DoubleType,  true) ::
      Nil)
}

// Schemas for DataMart.
object DM {

  val Orders =
    StructType(
      StructField("cust_id",     StringType,  false) ::
      StructField("orders_cnt",  IntegerType, false) ::
      StructField("payment_amt", DoubleType,  true)  ::
      StructField("days_old",    IntegerType, false) ::  // If registration date is not defined then equals to 0
      StructField("region",      IntegerType, false) ::
      Nil)
}
