/* sparkTest.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object sparkTest {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("sparkTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1 =  sc.textFile("/home/xs167-saurit/Desktop/Aspafka_db/SR_To_Do-list")
    rdd1.foreach(println)
  }
}
