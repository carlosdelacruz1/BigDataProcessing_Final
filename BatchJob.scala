package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUsersMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDevicesWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def countBytesPerAntenna(dataFrame: DataFrame): DataFrame
  def countBytesPerUser(dataFrame: DataFrame): DataFrame
  def countBytesPerApp(dataFrame: DataFrame): DataFrame
  def listUsersOverQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

}
