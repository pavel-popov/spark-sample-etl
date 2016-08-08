package com.megastartup.aux

object Options {
  def toOptionDouble(in: String): Option[Double] = {
    try {
      Some(in.trim.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }
}
