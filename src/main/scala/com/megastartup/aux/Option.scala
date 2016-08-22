package com.megastartup.aux

object Options {
  def toOptionDouble(in: String): Option[Double] = {
    try {
      Some(in.trim.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toOptionInt(in: String): Option[Int] = {
    try {
      Some(in.trim.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toOptionLong(in: String): Option[Long] = {
    try {
      Some(in.trim.toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }
}
