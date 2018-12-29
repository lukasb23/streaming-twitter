// Databricks notebook source
// MAGIC %md
// MAGIC #### Keeping Databricks clean

// COMMAND ----------

//%sh
//rm -rf /dbfs/tmp/*

// COMMAND ----------

//%sh
//rm -f /dbfs/FileStore/*.png

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
    withWatermark("datetime", "2 minutes").
    groupBy(window($"datetime", "60 seconds", "10 seconds"), $"city").
    count().
    select("window.start", "city", "count")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Writing into PostgreSQL Database
// MAGIC
// MAGIC PostgreSQL DB running on Azure. <br>
// MAGIC Notebook recipes under Notebook recipes under https://docs.databricks.com/_static/notebooks/structured-streaming-etl-kafka.html.

// COMMAND ----------

class JDBCSink(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
    
  val driver = "org.postgresql.Driver"
  var connection:java.sql.Connection = _
  var statement:java.sql.Statement = _

  def open(partitionId: Long, version: Long):Boolean = {
      Class.forName(driver)
      connection = java.sql.DriverManager.getConnection(url, user, pwd)
      statement = connection.createStatement
      true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    statement.executeUpdate("INSERT INTO public.tweets_new VALUES (nextval('w_sequence_new'), '" + value(0) + "', '" + value(1) + "'," + value(2) + ");")
  }

  def close(errorOrNull:Throwable):Unit = {
    connection.close
  }
}

// COMMAND ----------

val url="jdbc:postgresql://postgres-sparky.postgres.database.azure.com:5432/twitter?sslmode=require"
val user="<MY-USER>@postgres-sparky"
val pwd="<MY-PWD>"
val writer = new JDBCSink(url, user, pwd)

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val query = windowed_counts
  .writeStream
  .foreach(writer)
  .outputMode("complete")
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .start()

query.awaitTermination()
