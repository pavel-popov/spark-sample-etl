// attribution: https://github.com/mkuthan/example-spark
package com.megastartup.spec

import org.apache.spark.sql.SQLContext
import org.scalatest._

trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: SQLContext = _

  def sqlc: SQLContext = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }

}
