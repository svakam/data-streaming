# Databricks notebook source
# MAGIC %md ## Set up Connection to Apache Kafka

# COMMAND ----------

streamingInputDF = (
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "<server:ip>")
    .option("subscribe", "topic1")     
    .option("startingOffsets", "latest")
    .load()
  )

# COMMAND ----------

# MAGIC %md ## streamingInputDF.printSchema
# MAGIC 
# MAGIC   root <br><pre>
# MAGIC    </t>|-- key: binary (nullable = true) <br>
# MAGIC    </t>|-- value: binary (nullable = true) <br>
# MAGIC    </t>|-- topic: string (nullable = true) <br>
# MAGIC    </t>|-- partition: integer (nullable = true) <br>
# MAGIC    </t>|-- offset: long (nullable = true) <br>
# MAGIC    </t>|-- timestamp: timestamp (nullable = true) <br>
# MAGIC    </t>|-- timestampType: integer (nullable = true) <br>

# COMMAND ----------

# MAGIC %md ## Sample Message
# MAGIC <pre>
# MAGIC {
# MAGIC </t>"city": "<CITY>", 
# MAGIC </t>"country": "United States", 
# MAGIC </t>"countryCode": "US", 
# MAGIC </t>"isp": "<ISP>", 
# MAGIC </t>"lat": 0.00, "lon": 0.00, 
# MAGIC </t>"query": "<IP>", 
# MAGIC </t>"region": "CA", 
# MAGIC </t>"regionName": "California", 
# MAGIC </t>"status": "success", 
# MAGIC </t>"hittime": "2017-02-08T17:37:55-05:00", 
# MAGIC </t>"zip": "38917" 
# MAGIC }

# COMMAND ----------

# MAGIC %md ## GroupBy, Count

# COMMAND ----------

streamingSelectDF = (
  streamingInputDF
   .selectExpr("value::string:zip")
   .groupBy("zip") 
   .count()
  )

# COMMAND ----------

# MAGIC %md ## Window

# COMMAND ----------

from pyspark.sql.functions import window, col

streamingSelectDF = (
  streamingInputDF
   .selectExpr("value::string:zip", "value::string:hittime")
   .groupBy("zip", window(col("hittime").cast("timestamp"), "10 minute", "5 minute", "2 minute"))
   .count()
)


# COMMAND ----------

# MAGIC %md ## Memory Output

# COMMAND ----------

display(streamingSelectDF)

# COMMAND ----------

# MAGIC %md ## Write to a Delta table

# COMMAND ----------

streamingInputDF \
  .selectExpr("value::string:zip", "value::string:hittime", "value::string:status", "value::string:region") \
  .writeStream \
  .format("delta") \
  .trigger(processingTime="25 seconds") \
  .option("checkpointLocation", "/tmp/kafka-demo/_checkpoint") \
  .option("path", "/tmp/kafka-demo") \
  .table("kafka_demo")

# COMMAND ----------

# MAGIC %md ##### Select

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from kafka_demo

# COMMAND ----------

# MAGIC %md ## Write to a JDBC Sink

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql._
# MAGIC import org.apache.spark.sql.ForeachWriter
# MAGIC 
# MAGIC class JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, Long)] {
# MAGIC   val driver = "com.mysql.jdbc.Driver"
# MAGIC   var connection:Connection = _
# MAGIC   var statement:Statement = _
# MAGIC 
# MAGIC   def open(partitionId: Long,version: Long): Boolean = {
# MAGIC     Class.forName(driver)
# MAGIC     connection = DriverManager.getConnection(url, user, pwd)
# MAGIC     statement = connection.createStatement
# MAGIC     true
# MAGIC   }
# MAGIC 
# MAGIC   def process(value: (String, Long)): Unit = {
# MAGIC     statement.executeUpdate("INSERT INTO zip_test " + 
# MAGIC             "VALUES (" + value._1 + "," + value._2 + ")")
# MAGIC   }
# MAGIC 
# MAGIC   def close(errorOrNull: Throwable): Unit = {
# MAGIC     connection.close
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming.Trigger._
# MAGIC 
# MAGIC val url="jdbc:mysql://<mysqlserver>:3306/test"
# MAGIC val user ="user"
# MAGIC val pwd = "pwd"
# MAGIC 
# MAGIC val streamingInputDF = 
# MAGIC   spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "<server:ip>")
# MAGIC     .option("subscribe", "topic1")     
# MAGIC     .option("startingOffsets", "latest")
# MAGIC     .load()
# MAGIC 
# MAGIC val streamingSelectDF = 
# MAGIC   streamingInputDF
# MAGIC     .selectExpr("value::string:zip", "value::string:hittime")
# MAGIC     .groupBy("zip")
# MAGIC     .count()
# MAGIC     .as[(String, Long)]
# MAGIC 
# MAGIC val writer = new JDBCSink(url,user, pwd)
# MAGIC val query =
# MAGIC   streamingSelectDF
# MAGIC     .writeStream
# MAGIC     .foreach(writer)
# MAGIC     .outputMode("update")
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()

# COMMAND ----------

# MAGIC %md ## Kafka Sink

# COMMAND ----------

# MAGIC %md
# MAGIC ### Please see the official Apache Spark documentation for the latest Kafka integration best practices.
# MAGIC https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka
# MAGIC 
# MAGIC The minimum requirement of the data being written to Kafka is that there must be a column with the name `value` of type string or binary.

# COMMAND ----------

# DBTITLE 1,Creating a Kafka Sink for Streaming Queries
# MAGIC %scala
# MAGIC val query =
# MAGIC   streamingInputDF
# MAGIC     .writeStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "<server:ip>")
# MAGIC     .option("topic", "topic2")
# MAGIC     .option("checkpointLocation", "/path/to/checkpoint")
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()
