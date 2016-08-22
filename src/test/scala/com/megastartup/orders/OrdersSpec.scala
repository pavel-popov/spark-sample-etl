package com.megastartup.orders

import com.megastartup.spec._
import org.scalatest._

class OrdersSpec extends UnitSpec with SparkSpec {

  "Orders" should "be presented as string if payment is defined" in {
    Given("sample Orders('Pikachu', 14, Some(1234))")
    val input = Orders("Pikachu", 14, Some(1234))

    When("turning to string with ',' as separator")
    val result = input.mkString(",")

    Then("result string is created")
    result shouldBe "Pikachu,14,1234.00"
  }

  "Orders" should "be presented as string if payment is empty" in {
    Given("sample Orders('Bulbasaur', 14, None)")
    val input = Orders("Bulbasaur", 14, None)

    When("turning to string with ',' as separator")
    val result = input.mkString(",")

    Then("result string is created")
    result shouldBe "Bulbasaur,14,0.00"
  }
}
