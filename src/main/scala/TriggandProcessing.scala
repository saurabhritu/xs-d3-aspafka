import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

// *** use netcat's "nc -lk 9090" command in terminal to send data through socket ***

object TriggandProcessing {
  def main(args: Array[String]){

    val spark = SparkSession.builder()
      .appName("TriggandProcessing")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val Text_stream_DF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9090")
      .load()

    Text_stream_DF.printSchema()

    Text_stream_DF.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 second")) // use "Trigger.Once" to execute only once
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

  }

}
