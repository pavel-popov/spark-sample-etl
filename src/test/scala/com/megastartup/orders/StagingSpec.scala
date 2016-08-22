package com.megastartup.orders

import com.megastartup.spec._
import org.scalatest._

class StagingSpec extends UnitSpec with SparkSpec {

  "Data from Eighty" should "be staged" in {
    Given("sample data from Eighty")
    val input = Array(
      "SB,0,Республика Хакасия,2012-07-10,3,20101.97",
      "NW,0,Республика Коми,2013-04-01,3,89210.47",
      "NK,0,Карачаево-Черкесская Республика,2010-05-16,11,243413.57",
      "CE,0,Ярославская область,2015-05-11,6,19388.80",
      "GB,123,Тьмутаракань,2016-05-11,10,20000.80"
    )

    When("staging data")
    val result = Staging.fromEighty(sc.parallelize(input)).collect()

    Then("result is staged")
    result should equal(Array(
      Staging("SB0",   60, "2012-07-10", 3,  Some(20101.97)),
      Staging("NW0",   52, "2013-04-01", 3,  Some(89210.47)),
      Staging("NK0",   19, "2010-05-16", 11, Some(243413.57)),
      Staging("CE0",   85, "2015-05-11", 6,  Some(19388.80)),
      Staging("GB123",  0, "2016-05-11", 10, Some(20000.80))))
  }

  "Data from Minodo" should "be staged" in {
    Given("sample data from Minodo")
    val input = Array(
      "et@Riffpath.info,2,1436003685,10,8832.43",
      "rSanders@Leexo.name,2,1468241985,1,1146.48",
      "ToddGibson@Topicshots.info,1,1425932126,8,5291.74",
      "voluptate_vel@Zooveo.org,2,1464874707,2,1216.32",
      "aliquam_exercitationem@Innotype.org,1,1468685551,8,9267.39"
    )

    When("staging data")
    val result = Staging.fromMinodo(sc.parallelize(input)).collect()

    Then("result is staged")
    result should equal(Array(
      Staging("et@Riffpath.info",                    64, "2015-07-04", 10, Some(8832.43)),
      Staging("rSanders@Leexo.name",                 64, "2016-07-11", 1,  Some(1146.48)),
      Staging("ToddGibson@Topicshots.info",          30, "2015-03-09", 8, Some(5291.74)),
      Staging("voluptate_vel@Zooveo.org",            64, "2016-06-02", 2, Some(1216.32)),
      Staging("aliquam_exercitationem@Innotype.org", 30, "2016-07-16", 8, Some(9267.39))))
  }
}
