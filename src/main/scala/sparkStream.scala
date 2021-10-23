import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

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

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

//    *** this segments will also work fine ***
//    val words = lines.select(explode(split(lines("value"), " ")).alias("word"))
//    val wordCounts = words.groupBy("word").count()


    val query = wordCounts.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
}
