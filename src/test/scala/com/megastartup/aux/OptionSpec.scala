package com.megastartup.aux

import com.megastartup.spec._
import org.scalatest._

class OptionsSpec extends UnitSpec {
  "String containing decimal" should "be converted to Some value" in {
      Given("string with valid decimal 3.14")
      val input = "3.14"

      When("converting to OptionDouble")
      val result = Options.toOptionDouble(input)

      Then("result is Some(3.14)")
      result shouldBe Some(3.14)
    }

  "String containing integer" should "be converted to Some value" in {
      Given("string with valid integer 3")
      val input = "3"

      When("converting to OptionDouble")
      val result = Options.toOptionDouble(input)

      Then("result is Some(3.0)")
      result shouldBe Some(3.0)
    }

  "String containing text" should "be converted to None value" in {
      Given("string with text")
      val input = "asdf"

      When("converting to OptionDouble")
      val result = Options.toOptionDouble(input)

      Then("result is None")
      result shouldBe None
    }
}

