package com.megastartup.orders

import com.megastartup.spec._
import org.scalatest._

class CustomerSpec extends UnitSpec {

  "Customer" should "be presented as string" in {
    Given("sample Customer('Pikachu', '2016-08-11', 5)")
    val input = Customer("Pikachu", "2016-08-11", 5)

    When("turning to string with ',' as separator")
    val result = input.mkString(",")

    Then("result string is created")
    result shouldBe "Pikachu,2016-08-11,5"
  }
}

class CustomerMergerSpec extends UnitSpec with SparkSpec with SparkSqlSpec {
  "New customers" should "be added" in {
    Given("existing customers")
    val existingCustomers = Array(
      Customer("Pikachu", "2016-08-11", 5),
      Customer("Bulbasaur", "2016-01-11", 3),
      Customer("Drowsee", "2015-01-11", 1))

    Given("current period customers")
    val currentCustomers = Array(
      Customer("Pikachu", "2016-08-11", 5),
      Customer("Drowsee", "2015-01-11", 1),
      Customer("Charmander", "2013-01-11", 2),
      Customer("Smeargle", "2014-01-11", 4))

    When("merging customers")
    val (enrichedRDD, deltaRDD) = new CustomerMerger(sqlc).merge(
      sc.parallelize(existingCustomers),
      sc.parallelize(currentCustomers))

    val enriched = enrichedRDD.collect()
    val delta = deltaRDD.collect()

    Then("receiving full customers")
    enriched.toSet should equal(Array(
      Customer("Pikachu", "2016-08-11", 5),
      Customer("Bulbasaur", "2016-01-11", 3),
      Customer("Drowsee", "2015-01-11", 1),
      Customer("Charmander", "2013-01-11", 2),
      Customer("Smeargle", "2014-01-11", 4)).toSet)

    Then("delta should be be calculated")
    delta.toSet should equal(Array(
      Customer("Charmander", "2013-01-11", 2),
      Customer("Smeargle", "2014-01-11", 4)).toSet)
  }
}
