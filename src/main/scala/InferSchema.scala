// imports
import org.apache.spark.sql.SparkSession

object InferSchema {

  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("InferSchema")
      .config("spark.master", "local")
      .getOrCreate()

    // Always import this to use implicit functionality
    import spark.implicits._

    // Read from json and convert it into parquet
    val df_1 = spark.read.json("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.json")
    df_1.write.parquet("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.parquet")

    val parquetDF = spark.read.parquet("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.parquet")

//    parquetDF.createOrReplaceTempView("DSF_2_parquet")
//    val namesDF = spark.sql("SELECT * FORM DSF_2_parquet")
//    namesDF.map(attribute => "Name: " + attribute(0)).show()
    parquetDF.show()
  }
}
