package com.megastartup.orders

import com.megastartup.aux._

import org.apache.spark.rdd.RDD


object QualityGate {

  private def checkOrdersCnt(s: Array[String], idx: Int) = {
    if (Options.toOptionInt(s(idx)) != None) Some(s) else None
  }

  def checkItemQuality(src: RDD[String],
    passFunc: (String) => Boolean): (RDD[String], RDD[String]) = {

    val passed = src.filter(passFunc(_))
    val failed = src.filter(s => !passFunc(s))
    // TODO: get rid of multiple source reads

    (passed, failed)
  }

  def eightyItemPass(line: String): Boolean = {

    val checkLength = (x: Array[String]) => {
      if (x.length == 6) Some(x) else None
    }

    val checkDate = (x: Array[String]) => {
      if (ToDate.fromString(x(3)) != None) Some(x) else None
    }

    val ok = for {
      a <- Some(line.trim.split(","))
      b <- checkLength(a)
      c <- checkDate(b)
      d <- checkOrdersCnt(c, 4)
    } yield d

    ok match {
      case None => false
      case _    => true
    }
  }

  def minodoItemPass(line: String): Boolean = {

    val checkLength = (x: Array[String]) => {
      if (x.length == 5) Some(x) else None
    }

    val checkTimestamp = (x: Array[String]) => {
      if (Options.toOptionLong(x(2).trim) != None) Some(x) else None
    }

    val ok = for {
      a <- Some(line.trim.split(","))
      b <- checkLength(a)
      c <- checkTimestamp(b)
      d <- checkOrdersCnt(c, 3)
    } yield d

    ok match {
      case None => false
      case _    => true
    }
  }
}
