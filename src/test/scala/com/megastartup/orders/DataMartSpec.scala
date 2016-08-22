package com.megastartup.orders

import com.megastartup.spec._
import org.scalatest._
import org.apache.spark.sql.Row

class DataMartSpec extends UnitSpec with SparkSpec with SparkSqlSpec {

  "DataMart" should "be generated from Customers and Orders" in {
    Given("sample Customers")
    val customers = Array(
      Customer("Pikachu",    "2016-01-01", 5),
      Customer("Bulbasaur",  "2016-02-01", 3),
      Customer("Drowsee",    "2016-03-01", 1),
      Customer("Charmander", "2016-04-01", 2),
      Customer("Smeargle",   "2016-05-01", 4))

    Given("sample Orders")
    val orders = Array(
      Orders("Pikachu",    14, Some(13.10)),
      Orders("Bulbasaur",  34, Some(12345)),
      Orders("Charmander", 23, None),
      Orders("Smeargle",   10, Some(1111.01)))

    When("creating DataMart")
    val result = new DataMart(sqlc, "2016-08").generate(
      sc.parallelize(customers),
      sc.parallelize(orders)).collect()

    Then("result DataMart is created")
    result.toSet should equal(Array(
      Row("Pikachu",    14, 13.10,   213, 5),
      Row("Bulbasaur",  34, 12345.0, 182, 3),
      Row("Drowsee",    0,  0.0,     153, 1),
      Row("Charmander", 23, 0.0,     122, 2),
      Row("Smeargle",   10, 1111.01, 92,  4)).toSet)
  }

}
