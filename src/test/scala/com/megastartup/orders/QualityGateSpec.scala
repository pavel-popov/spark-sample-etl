package com.megastartup.orders

import com.megastartup.spec._
import org.scalatest._

class QualityGateSpec extends UnitSpec with SparkSpec {

  "Eighty data" should "be successfully cleaned up from bad data" in {
    Given("sample data")
    val input = Array(
      "BD,1,Bad Data 0,2012-07-10,asdf,20101.97",
      "BD,2,Bad Data 1,3,20101.97",
      "BD,3,Bad Data 2,3,20101.97,33",
      "BD,4,Bad Data 4,2012-07aaa-10,3,20101.97",
      "SB,0,Республика Хакасия,2012-07-10,3,20101.97",
      "NW,0,Республика Коми,2013-04-01,3,89210.47",
      "NK,0,Карачаево-Черкесская Республика,2010-05-16,11,243413.57",
      "CE,0,Ярославская область,2015-05-11,6,19388.80",
      "GB,123,Тьмутаракань,2016-05-11,10,20000.80"
    )

    When("passing raw data through Quality Gate")
    val (goodDataRDD, badDataRDD) = QualityGate.checkItemQuality(sc.parallelize(input), QualityGate.eightyItemPass)

    val goodData = goodDataRDD.collect()
    val badData = badDataRDD.collect()

    Then("good data should be preserved")
    goodData should equal(Array(
      "SB,0,Республика Хакасия,2012-07-10,3,20101.97",
      "NW,0,Республика Коми,2013-04-01,3,89210.47",
      "NK,0,Карачаево-Черкесская Республика,2010-05-16,11,243413.57",
      "CE,0,Ярославская область,2015-05-11,6,19388.80",
      "GB,123,Тьмутаракань,2016-05-11,10,20000.80"))

    Then("bad data should be extracted")
    badData should equal(Array(
      "BD,1,Bad Data 0,2012-07-10,asdf,20101.97",
      "BD,2,Bad Data 1,3,20101.97",
      "BD,3,Bad Data 2,3,20101.97,33",
      "BD,4,Bad Data 4,2012-07aaa-10,3,20101.97"))
  }

  "Minodo data" should "be successfully cleaned up from bad data" in {
    Given("sample data")
    val input = Array(
      "bad@data1.info,2,1436003685,10,8832.43,333",
      "bad@data2.info,2,143KK6003685,10,8832.43",
      "bad@data3.info,2,143KK6003685,1dd0,8832.43",
      "et@Riffpath.info,2,1436003685,10,8832.43",
      "rSanders@Leexo.name,2,1468241985,1,1146.48",
      "ToddGibson@Topicshots.info,1,1425932126,8,5291.74",
      "voluptate_vel@Zooveo.org,2,1464874707,2,1216.32",
      "aliquam_exercitationem@Innotype.org,1,1468685551,8,9267.39"
    )

    When("passing raw data through Quality Gate")
    val (goodDataRDD, badDataRDD) = QualityGate.checkItemQuality(sc.parallelize(input), QualityGate.minodoItemPass)

    val goodData = goodDataRDD.collect()
    val badData = badDataRDD.collect()

    Then("good data should be preserved")
    goodData should equal(Array(
      "et@Riffpath.info,2,1436003685,10,8832.43",
      "rSanders@Leexo.name,2,1468241985,1,1146.48",
      "ToddGibson@Topicshots.info,1,1425932126,8,5291.74",
      "voluptate_vel@Zooveo.org,2,1464874707,2,1216.32",
      "aliquam_exercitationem@Innotype.org,1,1468685551,8,9267.39"))

    Then("bad data should be extracted")
    badData should equal(Array(
      "bad@data1.info,2,1436003685,10,8832.43,333",
      "bad@data2.info,2,143KK6003685,10,8832.43",
      "bad@data3.info,2,143KK6003685,1dd0,8832.43"))
  }

}
