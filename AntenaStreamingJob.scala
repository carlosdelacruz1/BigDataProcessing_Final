package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.Duration


object AntenaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
  .builder()
  .master("local[20]")
  .appName("Ejercicio SQL Streaming")
  .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("bytes", LongType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("app", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false)
      )
    )
    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as("json"))
      .select("json.*")
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
      .withWatermark("timestamp","10 seconds") // 1 minute
      .groupBy($"antenna_id", window($"timestamp", "30 seconds").as("window")) // 5 minutes
      .agg(
        sum($"bytes").as("sum_antenna_bytes")
      )
      .select($"antenna_id", $"window.start".as("date"), $"sum_antenna_bytes")
  }
  override def countBytesPerUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withWatermark("timestamp", "10 seconds") // 1 minute
      .groupBy($"id", window($"timestamp", "30 seconds").as("window")) // 5 minutes
      .agg(
        sum($"bytes").as("sum_user_bytes")
      )
      .select($"id", $"window.start".as("date"), $"sum_user_bytes")
  }

  override def countBytesPerApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "10 seconds") // 1 minute
      .groupBy($"app", window($"timestamp", "30 seconds").as("window")) // 5 minutes
      .agg(
        sum($"bytes").as("sum_app_bytes")
      )
      .select($"app", $"window.start".as("date"), $"sum_app_bytes")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch{
        (batch: DataFrame, _: Long) => {
          batch
            .write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("url", jdbcURI)
            .option("dbtable", jdbcTable)
            .option("user", user)
            .option("password", password)
            .save()
        }
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"bytes", $"timestamp", $"app", $"id", $"antenna_id",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour"),
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation",s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val jdbcURI = "jdbc:postgresql://35.232.236.150:5432/postgres"
    val dbUser = "postgres"
    val dbPassword = "keepcoding"

    val kafkaDF = readFromKafka("34.125.22.106:9092", "devices")
    val parsedDF = parserJsonData(kafkaDF)
    val storageFuture = writeToStorage(parsedDF, "/tmp/devices_parquet/")
    val metadataDF = readUsersMetadata(jdbcURI,"user_metadata", dbUser, dbPassword)
    val enrichedDF = enrichDevicesWithMetadata(parsedDF, metadataDF)
    val aggBytesPerAntenna = countBytesPerAntenna(enrichedDF)
    val aggBytesPerUser = countBytesPerUser(enrichedDF)
    val aggBytesPerApp = countBytesPerApp(enrichedDF)

    val jdbcFutureAntenna = writeToJdbc(aggBytesPerAntenna, jdbcURI, "bytes_agg_antenna", dbUser, dbPassword)
    val jdbcFutureUser = writeToJdbc(aggBytesPerUser, jdbcURI, "bytes_agg_user", dbUser, dbPassword)
    val jdbcFutureApp = writeToJdbc(aggBytesPerApp, jdbcURI, "bytes_agg_app", dbUser, dbPassword)

    Await.result(
      Future.sequence(Seq(
        storageFuture,
        jdbcFutureAntenna,
        jdbcFutureUser,
        jdbcFutureApp
      )),Duration.Inf
    )
    spark.close()
  }
}
