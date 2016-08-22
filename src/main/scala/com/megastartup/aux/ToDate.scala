package com.megastartup.aux

import com.github.nscala_time.time.Imports._

object ToDate {

  val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")

  def fromTimestamp(ts: Long): String = {
    fmt.withZone(DateTimeZone.forID("Europe/Moscow")).print(ts*1000L)
  }

  def fromString(s: String): Option[String] = {
    try {
      val t = fmt.parseDateTime(s)
      Some(fmt.print(t))
    } catch {
      case _: Throwable => None
    }
  }
}
