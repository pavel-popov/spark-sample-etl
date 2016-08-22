package com.megastartup.lov

import com.megastartup.spec._
import org.scalatest._

class RegionsSpec extends UnitSpec {
  "Valid region name" should "be looked up in LOV" in {
      Given("region name 'Саратовская область'")
      val input = "Саратовская область"

      When("lookup region index")
      val result = Regions.ByName(input)

      Then("result is 65")
      result shouldBe 65
    }

  "Invalid region name" should "return 0 upon lookup" in {
      Given("region name 'Тьмутараканьская область'")
      val input = "Тьмутараканьская область"

      When("lookup region index")
      val result = Regions.ByName(input)

      Then("result is 0")
      result shouldBe 0
    }
}

