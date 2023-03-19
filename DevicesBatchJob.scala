package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.OffsetDateTime

object DevicesBatchJob extends BatchJob{
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Ejercicio Final STREAMING capag")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"$storagePath/data")
      .filter(
        $"year" === filterDate.getYear &&
        $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readUsersMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }
  override def enrichDevicesWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("a")
      .join(
        metadataDF.as("b"),
        $"a.id" === $"b.id"
      )
      .drop($"b.id")
  }

  override def countBytesPerAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .groupBy($"antenna_id", window($"timestamp", "1 hour").as("window"))
      .agg(
        sum($"bytes").as("sum_antenna_bytes")
      )
      .select($"antenna_id", $"window.start".as("date"), $"sum_antenna_bytes")
  }

  override def countBytesPerUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes", $"email")
      .groupBy($"id", $"email", window($"timestamp", "1 hour").as("window"))
      .agg(
        sum($"bytes").as("sum_user_bytes")
      )
      .select($"email", $"window.start".as("date"), $"sum_user_bytes")
  }

  override def countBytesPerApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour").as("window"))
      .agg(
        sum($"bytes").as("sum_app_bytes")
      )
      .select($"app", $"window.start".as("date"), $"sum_app_bytes")
  }

  override def listUsersOverQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes", $"email", $"quota")
      .groupBy($"id", $"email", $"quota", window($"timestamp", "1 hour").as("window"))
      .agg(sum($"bytes").as("sum_user_bytes"))
      .select($"email", $"window.start".as("date"), $"quota", $"sum_user_bytes")
      .filter($"quota" <  $"sum_user_bytes")
      //.select($"email", $"date")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  def main(args: Array[String]): Unit = {
    val jdbcURI = "jdbc:postgresql://35.232.236.150:5432/postgres"
    val dbUser = "postgres"
    val dbPassword = "keepcoding"

    val offsetDateTime = OffsetDateTime.parse("2022-10-27T12:00:00Z")
    val parquetDF = readFromStorage("/tmp/devices_parquet/",offsetDateTime)

    val metadataDF = readUsersMetadata(jdbcURI,"user_metadata", dbUser, dbPassword)
    val devicesEnrichedDF = enrichDevicesWithMetadata(parquetDF, metadataDF).cache()

    val aggBytesPerAntenna = countBytesPerAntenna(devicesEnrichedDF)
    writeToJdbc(aggBytesPerAntenna, jdbcURI,"bytes_agg_antenna_1h", dbUser, dbPassword)
    val aggBytesPerUser = countBytesPerUser(devicesEnrichedDF)
    writeToJdbc(aggBytesPerUser, jdbcURI,"bytes_agg_user_1h", dbUser, dbPassword)
    val aggBytesPerApp = countBytesPerApp(devicesEnrichedDF)
    writeToJdbc(aggBytesPerApp, jdbcURI,"bytes_agg_app_1h", dbUser, dbPassword)
    val usersOverQuota = listUsersOverQuota(devicesEnrichedDF)
    writeToJdbc(usersOverQuota, jdbcURI,"users_over_quota_1h", dbUser, dbPassword)

    spark.close()
  }
}
