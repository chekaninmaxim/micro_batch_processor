import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    println("Real-Time Streaming Data Pipeline Started ...")

    val KAFKA_TOPIC_NAME = "random_numbers"

// In-container configuration:
    val SPARK_MASTER_SERVER = "spark://spark-master:7077"
    val KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
    val HIVE_METASTORE_URI = "thrift://hive-metastore:9083"

    val interval = "5 seconds"
    val tableName = "test1.batch_mean_and_size"

// Local run configuration
//    val SPARK_MASTER_SERVER = "local[*]"
//    val KAFKA_BOOTSTRAP_SERVER = "localhost:9093"
//    val HIVE_METASTORE_URI = "thrift://localhost:9083"

    val spark = SparkSession.builder
      .master(SPARK_MASTER_SERVER)
      .appName("Real-Time Streaming Data Pipeline")
      .config("hive.metastore.uris", HIVE_METASTORE_URI)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val messages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
      .option("subscribe", KAFKA_TOPIC_NAME)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of event_message_detail_df: ")
    messages.printSchema()
    // Code Block 3 Ends Here

    // Code Block 4 Starts Here
    val messageSchema = StructType(Array(
      StructField("id", StringType),
      StructField("timestamp", StringType),
      StructField("value", IntegerType)
    ))

    // Code Block 5 Starts Here
    val parsedMessageDf = messages
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), messageSchema).alias("message_object"))
      .select("message_object.*")

    print("Printing Schema of parsed messages: ")
    parsedMessageDf.printSchema()

    parsedMessageDf.writeStream
      .trigger(Trigger.ProcessingTime(interval))
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Write data from spark dataframe to database/table
        batchDF.agg(
            avg("value").alias("batch_mean"),
            max("timestamp").alias("processing_timestamp"),
            count("value").alias("batch_size")
          ).select(
            lit(batchId).alias("batch_id"),
            col("batch_mean"),
            col("processing_timestamp"),
            col("batch_size")
          )
          .write.mode(SaveMode.Append)
          .format("hive")
          .saveAsTable(tableName)


      }.start()

    val parsedMessageWriteStream = parsedMessageDf
      .writeStream
      .trigger(Trigger.ProcessingTime(interval))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()

    parsedMessageWriteStream.awaitTermination()

    println("Real-Time Streaming Data Pipeline Completed.")
  }
}
