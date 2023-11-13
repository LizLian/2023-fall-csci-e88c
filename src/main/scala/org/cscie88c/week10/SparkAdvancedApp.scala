package org.cscie88c.week10

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import org.cscie88c.utils.SparkUtils
import pureconfig.generic.auto._
import org.apache.spark.sql.{Dataset, DataFrame, SaveMode}
import cats._
import cats.implicits._

// run with:
// ./bin/spark-submit --class "org.cscie88c.week10.SparkAdvancedApp" --master local[*] /opt/spark-apps/SparkAdvancedApp.jar <input file> <output file>
object SparkAdvancedApp extends LazyLogging {

case class AdvancedSparkAppSettings(name: String, masterUrl: String, transactionFile: String, month: String)

  def main(args: Array[String]): Unit = {
    logger.info("XXXX: Start of Advanced SparkApp")

    // 1. read configuration
    val conf = ConfigUtils.loadAppConfig[AdvancedSparkAppSettings]("org.cscie88c.advanced-spark-app")
    val Array(inputFile, outputPath) = args
    logger.info(s"settings: $conf")
    implicit val spark = SparkUtils.sparkSession(conf.name, conf.masterUrl)

    // 2. load the data
    val ds: Dataset[RawTransaction] = loadData(inputFile)

    // 3. transform the data
    val averageById: Map[String,AverageTansactionAggregate] = aggregateDataWithMonoid(ds)

    // 4. write results
    saveAverageTransactionByCustomerId(averageById, outputPath)

    spark.stop()
    logger.info("XXXX: Stopped Advanced SparkApp")
  }

  def loadData(inputFile: String)(implicit spark: SparkSession):Dataset[RawTransaction] = {
    import spark.implicits._
    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputFile)
      .as[RawTransaction]
  }

  def aggregateDataWithMonoid
    (transactionDS: Dataset[RawTransaction])
    (implicit spark: SparkSession): Map[String,AverageTansactionAggregate] = {
    import spark.implicits._
    transactionDS
      .map(raw => Map(raw.customer_id -> AverageTansactionAggregate(raw)))
      .reduce(_ |+| _)
  }

  def saveAverageTransactionByCustomerId
    (transactionsById: Map[String,AverageTansactionAggregate], path: String)
    (implicit spark: SparkSession)
    : Unit = {
    import spark.implicits._
    transactionsById.foreach { 
      case (customerId: String, averageTransaction:AverageTansactionAggregate) =>
        logger.info(s"[XXXX]: ${customerId}: ${averageTransaction}")
    }
    val outputRows: DataFrame = transactionsById.map {
      case (customerId: String, averageTransaction:AverageTansactionAggregate) =>
        AggregateResult(customerId, averageTransaction.averageAmount)
    }.toList.toDF()

    outputRows
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

}

