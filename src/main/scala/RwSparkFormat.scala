import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

//*** Always remember file format which store  streaming data always have folder "meta_data" ***

object RwSparkFormat {

  def main(args: Array[String]){

    val spark = SparkSession.builder()
      .appName("RwSparkFormat")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

//    *** Create a custom dataframe ***
//    val empsalary = Seq(
//      Salary("sales", 1, 5000),
//      Salary("personnel", 2, 3900),
//      Salary("sales", 3, 4800),
//      Salary("sales", 4, 4800),
//      Salary("personnel", 5, 3500),
//      Salary("develop", 7, 4200),
//      Salary("develop", 8, 6000),
//      Salary("develop", 9, 4500),
//      Salary("develop", 10, 5200),
//      Salary("develop", 11, 5200)).toDF()
//
//    empsalary.show()

//    *** Streaming Data ***
//    val Text_stream = spark.readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", "9090")
//      .load()
//
//    Text_stream.printSchema()

//    *** R/W into parquet ***
//    val filepath = "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/RWSF/Forquet/"

//    Text_stream.writeStream
//      .outputMode("append")
//      .format("parquet")
//      .option("truncate", false)
//      .option("checkPointLocation", "tmp/checkpoint/RWSF/cparquet")
//      .option("path", filepath)
//      .start()
//      .awaitTermination()

//    empsalary.write
//      .format("parquet")
//      .option("truncate", false)
//      .mode("overwrite")
//      .save(filepath)

//    val sampleDF = spark.read.format("parquet").load(filepath)
//
//    sampleDF.show(false)

//    *** R/W into avro ***
//    val filepath = "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/RWSF/Foravro/"

//    Text_stream.writeStream
//      .outputMode("append")
//      .format("avro")
//      .option("truncate", false)
//      .option("checkPointLocation", "tmp/checkpoint/RWSF/cavro")
//      .option("path", filepath)
//      .start()
//      .awaitTermination()

//    empsalary.write
//      .format("avro")
//      .option("truncate", false)
//      .mode("overwrite")
//      .save(filepath)

//    val sampleDF = spark
//      .read
//      .format("avro")
//      .load(filepath)
//
//    sampleDF.show(false)

//    *** R/W into json ***
//    val filepath = "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/RWSF/Forjson/"

//    Text_stream.writeStream
//      .outputMode("append")
//      .format("json")
//      .option("truncate", false)
//      .option("checkPointLocation", "tmp/checkpoint/RWSF/cjson")
//      .option("path", filepath)
//      .start()
//      .awaitTermination()

//    empsalary.write
//      .format("json")
//      .option("truncate", false)
//      .mode("overwrite")
//      .save(filepath)

//    val sampleDF = spark
//      .read
//      .format("json")
//      .load(filepath)
//
//    sampleDF.show(false)

//    *** R/W into json ***
//    val filepath = "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/RWSF/Forcsv/"

//    Text_stream.writeStream
//      .outputMode("append")
//      .format("csv")
//      .option("truncate", false)
//      .option("checkPointLocation", "tmp/checkpoint/RWSF/ccsv")
//      .option("path", filepath)
//      .start()
//      .awaitTermination()

//    empsalary.write
//      .format("csv")
//      .option("truncate", false)
//      .mode("overwrite")
//      .save(filepath)

//    *** Enable this schema to get data with meaningful column name ***
//    val csv_schema = new StructType()
//      .add("depName", "string")
//      .add("empNo", "integer")
//      .add("salary", "long")


//    val sampleDF = spark
//      .read
//      .format("csv")
//      .schema(csv_schema)
//      .load(filepath)

//    sampleDF.show(false)

  }

}
