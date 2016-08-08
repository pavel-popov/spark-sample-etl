package com.megastartup.orders

import org.apache.spark.rdd.RDD

import com.megastartup.lov.Regions
import com.megastartup.aux.Options.toOptionDouble

case class OrderStg(
  CustID: String,
  Region: Int,
  RegistrationDate: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double]
)

object OrderStg {

  def fromEighty (src: RDD[String]): RDD[OrderStg] = {
    src.map(_.split(","))
      .map(p => OrderStg( p(0).trim+p(1).trim,          // branch+client_id
                          Regions.ByName(p(2).trim),    // lookup region id by name
                          p(3).trim,                    // TODO: add date parsing
                          p(4).trim.toInt,
                          toOptionDouble(p(5)) ))
  }

  def fromMinodo (src: RDD[String]): RDD[OrderStg] = {

    val regionIndex = (i: String) => {
      val name = i match {
        case "1" => "Москва"
        case "2" => "Санкт-Петербург"
        case  _  => "N/A"
      }
      Regions.ByName(name)
    }

    src.map(_.split(","))
      .map(p => OrderStg( p(0).trim,                    // email
                          regionIndex(p(1).trim),       // lookup region id by index
                          p(2).trim,                    // TODO: add convertioni from timestamp to date
                          p(3).trim.toInt,
                          toOptionDouble(p(4)) ))

  }

}

case class CustomerDim(
  CustID: String,
  RegistrationDate: String,
  Region: Int
)

case class OrderFact(
  CustID: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double]
)

case class OrderDM(
  CustID: String,
  OrdersCnt: Int,
  PaymentSum: Option[Double],
  DaysOld: Int,
  Region: Int
)
