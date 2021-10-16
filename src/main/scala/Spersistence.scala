import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Spersistence {
  def main(args: Array[String]){
     val spark = SparkSession.builder()
       .appName("Spersistence")
       .config("spark.master", "local")
       .getOrCreate()

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
    rdd2.foreach(println)
    rdd3.foreach(println)
  }
}
