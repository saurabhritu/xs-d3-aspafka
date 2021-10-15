// imports
import org.apache.spark.sql.SparkSession

object Spark_DSF {

  def main(args:Array[String]){
    val spark = SparkSession
      .builder()
      .appName("Spark_DSF")
      .config("spark.master", "local")
      .getOrCreate()
    // Becareful about json file structure
    val df = spark.read.json("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_1.json")
    df.show()
  }
}