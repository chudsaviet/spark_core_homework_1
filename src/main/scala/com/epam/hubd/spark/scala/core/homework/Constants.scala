package com.epam.hubd.spark.scala.core.homework

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone


object Constants {

  val DELIMITER = ","

  val BIDS_HEADER = Seq("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
  val MOTELS_HEADER = Seq("MotelID", "MotelName", "Country", "URL", "Comment")
  val EXCHANGE_RATES_HEADER = Seq("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy").withZone(DateTimeZone.forID("Europe/Budapest"))
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm").withZone(DateTimeZone.forID("Europe/Budapest"))
}
