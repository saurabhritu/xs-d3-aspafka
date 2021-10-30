import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Spersistence {
  def main(args: Array[String]){

     val spark = SparkSession.builder()
       .appName("Spersistence")
       .config("spark.master", "local")
       .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
//    *** Ways to create rdd from Parallelized Connection, External Data, From RDD ***

    val rdd0 = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9))
    val rdd1 = rdd0.map(_ + 2) // or map(x=>x+2)
    rdd1.foreach(println)

    // using persistence
    rdd1.persist(StorageLevel.MEMORY_ONLY_SER)

    // filtering even numbers
    val rdd2 = rdd1.filter(x => x%2 == 0)
    val rdd3 = rdd1.filter(x=> x%2 != 0 )
    rdd2.foreach(print)
    println()
    rdd3.foreach(print)

    val rdd4 = rdd1.toDF()
    val filePath = "/home/saurabh/Desktop/Spafka_RW/Write/Persistence_write/test_csv_1/"

    rdd4.write
      .format("csv")
      .option("trucate", false)
      .mode("overwrite")
      .save(filePath)

    val filePath_2 = "/home/saurabh/Desktop/Spafka_RW/Write/Persistence_write/test_json_2/"

    rdd4.write
      .format("json")
      .option("trucate", false)
      .mode("overwrite")
      .save(filePath_2)
  }
}
