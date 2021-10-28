// imports
import org.apache.spark.sql.SparkSession
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.Partitioner
import org.apache.spark.sql.functions.broadcast

//case class CP extends Partitioner {
//  var arg = 0
//  CP(){
//    return a;
//  }
//}

object InferSchema {

  def main(args: Array[String]){

    val spark = SparkSession.builder()
      .appName("InferSchema")
      .config("spark.master", "local[5]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Always import this to use implicit functionality
    import spark.implicits._

    // *** Read from json and convert it into parquet ***
//    val df_1 = spark.read.json("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.json")
//    df_1.write.parquet("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.parquet")

//    val parquetDF = spark.read.parquet("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.parquet")

//    parquetDF.createOrReplaceTempView("DSF_2_parquet")
//    val namesDF = spark.sql("SELECT * FORM DSF_2_parquet")
//    namesDF.map(attribute => "Name: " + attribute(0)).show()
//    parquetDF.show()

//    *** Read data from csv with/without schema ***

//    val csvSchema =  new StructType()
//      .add("id", "Integer")

//    val df_2 = spark.read
//      .option("header", true )
//      .csv("/home/saurabh/Desktop/Spafka_RW/CSV_RW/Read/CSV_1.csv")

//    val df_2 = spark.read
//      .option("header", false )
//      .option("inferSchema", true)
//      .csv("/home/saurabh/Desktop/Spafka_RW/CSV_RW/Read/CSV_1.csv")

//    df_2.printSchema()
//    df_2.show()

//    *** Repartition and Coalesce Example ***
//    val rdd_1 = spark.sparkContext.parallelize(Range(0,20))
//    println(rdd_1.partitions.size)
//
//    for( i <- 0 to 4) {
//      println("Default Partitions local[5] : " + rdd_1.partitions(i))
//    }
//        rdd_1.saveAsTextFile("/tmp/partition")
//
//    val rdd_2 = rdd_1.repartition(6)
//    println("repartition [6]" + rdd_2.partitions.size)
//
//    for( i <- 0 to 5) {
//      println(rdd_2.partitions(i))
//    }
//
//    rdd_2.saveAsTextFile("/tmp/repartition")
//
//    val rdd_3 = rdd_2.coalesce(4)
//    println( "Coalesce [4]: " + rdd_3.partitions.size)
//
//    for(i <- 0 to 3 ){
//      println(rdd_3.partitions(i))
//    }
//
//    rdd_3.saveAsTextFile("/tmp/coalesce")

    // *** Repartition by key *** ### Under development
//    val rdd_4 = spark.sparkContext.parallelize(Range(0,20)).map( ((x: Int) => (x%3, x)))
//    println(rdd_4.partitions.size)
//    rdd_4.foreach(println)

//    val rdd_5 = rdd_4.partitionBy()

    // *** Broadcast Join ***
    val peopleDF = Seq(
      ("saurabh", "Patna"),
      ("Stacksr", "Vancoover"),
      ("Riya", "Kolkata"),
      ("Yash", "Berlin")
    ).toDF("Name", "City")

    peopleDF.show()

    val citiesDF = Seq(
      ("Patna", "India", 4),
      ("Kolkata", "India", 5),
      ("Vancoover", "Canada", 2),
      ("Berlin", "Geramany", 3)
    ).toDF("City", "Country", "Population( Cr. )")

    citiesDF.show()

    peopleDF.join(
      broadcast(citiesDF),
      peopleDF("city") <=> citiesDF("city")
    ).drop(citiesDF("city"))
      .show()

    peopleDF.join(
      broadcast(citiesDF),
      peopleDF("city") <=> citiesDF("city")
    ).drop(citiesDF("city"))
      .explain(true)

  }
}