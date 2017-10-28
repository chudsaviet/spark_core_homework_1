package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    return sc.textFile(bidsPath)
      .map(x => x.split(",").toList)
}

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    return rawBids
      .filter(x => x(2).slice(0,6) == "ERROR_")
      .map(x => (x.slice(1,3), 1))
      .reduceByKey((x, y) => x+y)
      .map(x => (x._1 :+ x._2.toString()).mkString(","))
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    return sc.textFile(exchangeRatesPath)
      .map(x => {
        val l = x.split(",")
        (l(0),l(3).toDouble)
      })
      .collectAsMap()
      .toMap // To *immutable* map. Freaking Scala.
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    def extractBid(row: List[String], pricePosition: Int, loSa: String): BidItem = {
      return new BidItem(
        motelId = row(0),
        bidDate =
          Constants.INPUT_DATE_FORMAT.parseDateTime(row(1)).toString(Constants.OUTPUT_DATE_FORMAT),
        loSa = loSa,
        // Sorry for StackOverflow hack to limit precision
        price =  Math.round(row(pricePosition).toDouble * exchangeRates(row(1))*1000)/1000.0d
      )
    }
    def extractSpecifiedBidItems(row: List[String]): TraversableOnce[BidItem] = {
      // Wish to do it more elegant way using 'yield'
      // But I don't know Scala enough :(
      val buffer = new ListBuffer[BidItem]
      // Just suppress NumberFormatExceptions and NoSuchElementException, don't add such columns to buffer
      // Order *SHOULD* be US-CA-MX, otherwise unit test will not pass
      // Killed an hour for this
      try {
        buffer += extractBid(row, 5, "US")
      } catch {
        case _:java.lang.NumberFormatException =>
        case _:java.util.NoSuchElementException =>
      }
      try {
        buffer += extractBid(row, 8, "CA")
      } catch {
        case _:java.lang.NumberFormatException =>
        case _:java.util.NoSuchElementException =>
      }
      try {
        buffer += extractBid(row, 6, "MX")
      } catch {
        case _:java.lang.NumberFormatException =>
        case _:java.util.NoSuchElementException =>
      }
      return buffer
    }

    return rawBids
      .filter(x => x(2).slice(0,6) != "ERROR_")
      .flatMap(extractSpecifiedBidItems)
  }

  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = {
    return sc.textFile(motelsPath)
      .map(x => {
        val list = x.split(",", 3)
        (list(0), list(1))
      })
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {

    def maxElement(a: Tuple2[EnrichedItem, Long], b: Tuple2[EnrichedItem, Long]): Tuple2[EnrichedItem, Long] = {
      val item_a = a._1
      val index_a = a._2
      val item_b = b._1
      val index_b = b._2
      if (item_a.price > item_b.price)
        return a
      else if (item_a.price < item_b.price)
        return b
      else {
        if (index_a < index_b)
          return a
        else return b
      }
    }

    val bidsPrepared=bids.map(x => (x.motelId, x))
    val enrichedItems = bidsPrepared.join(motels).map(x => {
      val row = x._2
      new EnrichedItem(
        motelId = row._1.motelId,
        motelName = row._2,
        bidDate = row._1.bidDate,
        loSa = row._1.loSa,
        price = row._1.price
      )
    })
    return enrichedItems
      .zipWithIndex
      .cache
      .map( x => ((x._1.motelId, x._1.bidDate),x))
      .reduceByKey(maxElement)
      .map( x => x._2._1)
  }
}
