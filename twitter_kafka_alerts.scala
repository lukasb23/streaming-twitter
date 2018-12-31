// Databricks notebook source
// MAGIC %md
// MAGIC #### Keeping Databricks clean

// COMMAND ----------

// MAGIC %sh
// MAGIC rm -rf /dbfs/tmp/*

// COMMAND ----------

// MAGIC %sh
// MAGIC rm -f /dbfs/FileStore/*.png

// COMMAND ----------

// MAGIC %md
// MAGIC #### Constructing the stream

// COMMAND ----------

// Public IP and port of VM (running Kafka)
val public_ip_port = "<MY-IP>:9092"
val kafka_input_topic = "twitter-in"

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = (new StructType)
  .add("tweet", StringType, false)
  .add("city", StringType, false)
  .add("datestring", StringType, false)
  .add("hashtags", StringType, true)

// COMMAND ----------

import org.apache.spark.sql.functions.{from_json, col, unix_timestamp, to_timestamp}

val tweets = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", public_ip_port)
             .option("subscribe", kafka_input_topic)
             .option("startingOffsets", "latest")
             .load()
             .selectExpr("topic", "timestamp", "partition", "offset", "CAST(key as STRING)", "CAST(value AS STRING)")
             .select(from_json($"value",schema).as("values"))
             .select($"values.*")
             .withColumn("datetime", to_timestamp(unix_timestamp(col("datestring"), "yyyy-MM-dd HH:mm:ss")))
             .drop($"datestring")
            )

// COMMAND ----------

// windowed counts incl. watermark to clear old state

import spark.implicits._
import org.apache.spark.sql.functions.{window}

val windowed_counts = tweets.
    withWatermark("datetime", "10 minutes").
    groupBy(window($"datetime", "60 seconds", "10 seconds"), $"city").
    count().
    select("window.start", "city", "count")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Writing alerts back into Kafka
// MAGIC
// MAGIC - *Creating Alert Topic:* bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter-alerts
// MAGIC - Sending alerts to Kafka topic if count > thresholds / min

// COMMAND ----------

val kafka_output_topic = "twitter_alerting"

val threshold_paris = 8
val threshold_rome = 7
val threshold_rest = 5

val remaining_cities = List("Zurich", "Vienna", "Berlin", "Munich")

// COMMAND ----------

val windowed_alerts = tweets.
    withWatermark("datetime", "5 seconds").
    groupBy(window($"datetime", "60 seconds", "10 seconds"), $"city").
    count().
    select("window.start", "city", "count").
    filter(($"count" >= threshold_paris && $"city" === "Paris") ||
           ($"count" >= threshold_rome && $"city" === "Rome") || 
           ($"count" >= threshold_rest && $"city" =!= "Rome" && $"city" =!= "Paris"))

// COMMAND ----------

import org.apache.spark.sql.functions.{concat, lit}

windowed_alerts.select(lit("0").as("key"), concat(lit("{'city': "), col("city"), lit(", "),
                                                  lit("'window_start': "), col("start"), lit(", "),
                                                  lit("'effective_count': "), col("count"), lit(" }")).as("value"))
  .writeStream.format("kafka")
  .option("kafka.bootstrap.servers", public_ip_port)
  .option("topic", kafka_output_topic)
  .option("checkpointLocation", "tmp/checkpoint02")
  .outputMode("append").start()
