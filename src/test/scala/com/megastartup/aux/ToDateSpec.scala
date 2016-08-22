package com.megastartup.aux

import com.megastartup.spec._
import org.scalatest._

class ToDateSpec extends UnitSpec {
  "Long containing timestamp within a day" should "be converted to YYYY-MM-DD string in Moscow Timezone" in {
      Given("timestamp 1471839825 which is 2016-08-22T04:23:45+00:00")
      val input = 1471839825L

      When("converting to Date string")
      val result = ToDate.fromTimestamp(input)

      Then("result is 2016-08-22")
      result shouldBe "2016-08-22"
    }

  "Long containing timestamp near 00:00 UTC" should "be converted to YYYY-MM-DD string in Moscow Timezone" in {
      Given("timestamp 1471823000 which is 2016-08-21T23:43:20+00:00")
      val input = 1471823000L

      When("converting to Date string")
      val result = ToDate.fromTimestamp(input)

      Then("result is 2016-08-22")
      result shouldBe "2016-08-22"
    }

  "String containing date" should "be checked" in {
      Given("string '2016-05-12'")
      val input = "2016-05-12"

      When("checking that it's a date")
      val result = ToDate.fromString(input)

      Then("result is Some(2016-05-12)")
      result shouldBe Some("2016-05-12")
    }

  "String containing invalid date" should "be marked as None" in {
      Given("string '2016-05aaa-12'")
      val input = "2016-05aaa-12"

      When("checking that it's a date")
      val result = ToDate.fromString(input)

      Then("result is None")
      result shouldBe None
    }
}

