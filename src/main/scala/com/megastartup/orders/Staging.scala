package com.megastartup.orders

import org.apache.spark.rdd.RDD

import com.megastartup.lov.Regions
import com.megastartup.aux.Options.toOptionDouble
import com.megastartup.aux.ToDate

case class Staging(
  CustID: String,
  Region: Int,
  RegistrationDate: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double]
)

object Staging {

  def fromEighty (src: RDD[String]): RDD[Staging] = {
    src.map(_.split(","))
      .map(p => Staging( p(0).trim+p(1).trim,           // branch+client_id
                          Regions.ByName(p(2).trim),    // lookup region id by name
                          p(3).trim,                    // TODO: add date parsing
                          p(4).trim.toInt,
                          toOptionDouble(p(5)) ))
  }

  def fromMinodo (src: RDD[String]): RDD[Staging] = {

    val regionIndex = (i: String) => {
      val name = i match {
        case "1" => "Москва"
        case "2" => "Санкт-Петербург"
        case  _  => "N/A"
      }
      Regions.ByName(name)
    }

    src.map(_.split(","))
      .map(p => Staging( p(0).trim,                    // email
                         regionIndex(p(1).trim),       // lookup region id by index
                         ToDate.fromTimestamp(p(2).trim.toLong),   // TODO: add convertion from timestamp to date
                         p(3).trim.toInt,
                         toOptionDouble(p(4)) ))
  }
}

