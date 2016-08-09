package com.megastartup.schemas

import org.apache.spark.sql.types._

object Customer {
  val customer =
    StructType(
      StructField("CustID",           StringType,  false) ::
      StructField("RegistrationDate", StringType,  true) ::
      StructField("Region",           IntegerType, true) ::
      Nil)
}
