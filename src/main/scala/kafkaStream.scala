import java.io.File
import org.apache.avro.Schema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.StructType

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object kafkaStream {
def main(args: Array[String]) {

  val conf = new SparkConf().setMaster("local").setAppName("RDD_Demo")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder()
    .appName("kafkaStream")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

// *** Read/Write Avro data in spark ***
//  spark.read.format("avro").load("/home/saurabh/Desktop/Spafka_RW/avro/userdata1.avro").show()
//
//  val userdataSchema = new Schema.Parser()
//    .parse(new File("/home/saurabh/Desktop/Spafka_RW/avro/userdata.avsc"))
//
//  val df = spark.read.format("avro")
//    .option("userdataSchema", userdataSchema.toString)
//    .load("/home/saurabh/Desktop/Spafka_RW/avro/userdata1.avro")
//
//  df.createOrReplaceTempView("userdataDF")
//  val sqldf = spark.sql("SELECT first_name FROM userdataDF WHERE first_name LIKE 'K%' ")
//
//  sqldf.show()


// *** Connect to kafka Stream ***
  val kdf = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test_1")
    .option("startingOffsets", "earliest")
    .load()

  kdf.printSchema()

  // *** Read json from kafka stream without schema (Value only) ***
  val dfk = kdf.selectExpr("CAST(value AS STRING)")

  dfk.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start().awaitTermination()


// *** Read json from kafka stream with schema ***

//  val dfk = kdf.selectExpr("CAST(value AS STRING)")
//
//  val dfkSchema = new StructType()
//    .add("name", "String")
//    .add("address", "String")
//
//  val personDfk = dfk.select(from_json(col("value"), dfkSchema).as("data"))
//    .select("data.*")
//
//  personDfk.writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//    .awaitTermination()

//  personDfk.show()

}
}
