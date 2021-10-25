import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType

// *** use netcat's "nc -lk 9090" command in terminal to send data through socket ***

object sparkStream {

  def main(args: Array[String]){

    val spark = SparkSession
      .builder()
      .appName("sparkStream")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load()

    lines.printSchema()

//    *** General Main Code ***
//    val words = lines.as[String].flatMap(_.split(" "))
//    val wordCounts = words.groupBy("value").count()

//    *** this segments will also work fine ***
//    val words = lines.select(explode(split(lines("value"), " ")).alias("word"))
//    val wordCounts = words.groupBy("word").count()

//    *** using watermark for writing into file format ***
    val words = lines.as[String].flatMap(_.split(" "))
      .withColumn("timestamp", current_timestamp())
    val wordCounts = words
      .withWatermark("timestamp", "2 minutes")
      .groupBy(window($"timestamp", "2 minutes", "1 minutes"), $"value")
      .count()

//    *** Writing streaming data to console ***
    val query = wordCounts.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()

//    *** Writing streaming data to kafka stream ***
//    val DFcounts = wordCounts.selectExpr("CAST(value AS STRING)", "CAST(count AS STRING)")
//
//        DFcounts.selectExpr("to_json(struct(*)) AS value")
//          .writeStream
//          .format("kafka")
//          .outputMode("complete")
//          .option("kafka.bootstrap.servers", "localhost:9092")
//          .option("topic", "counts")
//          .option("checkpointLocation", "tmp/checkpoint/sparkStream/DFcounts/")
//          .start()
//          .awaitTermination()


//    *** Writing Streaming data in files system ***
//    lines.isStreaming
//
//    val DFcounts = lines.selectExpr("CAST(value AS STRING)")
//    val DFcounts_schema = new StructType()
//      .add("word", "string")
//      .add("count", "integer")
//
//    val count = DFcounts.selectExpr("to_csv(struct(*)) AS value")
//      .writeStream
//      .outputMode("append")
//      .format("csv") // supports csv, json, parquet & orc
//      .option("path", "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/text.csv") // count.json
//      .option("checkpointLocation", "tmp/checkpoint/sparkStream/NonAgg/")
//      .start()
//      .awaitTermination()


//    *** Writing aggregated streaming data into file system using watermarking & windowing [ going on..! ]***

//    lines.isStreaming
//    val DFcounts = wordCounts.selectExpr("CAST(value AS STRING)", "CAST(count AS STRING)")
//    val DFcounts_schema = new StructType()
//      .add("word", "string")
//      .add("count", "integer")
//
//    val count = DFcounts.selectExpr("to_csv(struct(*)) AS value")
//      .writeStream
//      .outputMode("append")
//      .format("csv") // supports csv, json, parquet & orc
//      .option("path", "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/count")
//      .option("checkpointLocation", "tmp/checkpoint/sparkStream/counts/")
//      .start()
//      .awaitTermination()

  }
}
