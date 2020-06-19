package db.util

import java.time.{Instant, LocalDate, ZoneId, ZoneOffset}

object DateUtils {

  def toMillis(date: LocalDate): Long = date.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli

  def fromMillis(millis: Long): LocalDate = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).toLocalDate

}
