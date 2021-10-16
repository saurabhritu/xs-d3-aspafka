// imports
import org.apache.spark.sql.SparkSession

case class Person(id: String, name: String, team: String )

object Spark_DSF {

  def main(args:Array[String]){
    val spark = SparkSession
      .builder()
      .appName("Spark_DSF")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    // json file structure must structured so that it can be converted into dataframes
    val df = spark.read.json("/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_1.json")
    df.show()
//    ### Useful code uncomment for running ###

//    *** SQL Operation ***
//    df.printSchema()
//    df.select("name").show()
//    df.select("address").show()
//    df.filter($"name" === "Saurabh").select("address").show()
//    df.groupBy("address").count().show()

//    *** concept of temp view ***
//    df.createOrReplaceTempView("people")
//    val sqlDF = spark.sql("SELECT * FROM people WHERE name = 'Saurabh'")
//    sqlDF.show()

//    *** concept of Global_temp_View***
//    df.createGlobalTempView("people")
//    val gsqlDF = spark.sql("SELECT * FROM global_temp.people WHERE name = 'Saurabh'")
//    gsqlDF.show()

//      *** Creating Dataset from case classes *** [ Encoders are created for case classes ]
//    val caseClassDS = Seq(Person(1, "Riot", "Cloud"), Person(2, "valorant", "DataOps")).toDF()
//    caseClassDS.show()

//    *** Encoders for most common types are automatically provided by spark.implicits._ ***
//    val primitiveDS = Seq(1,2,3).toDS() // just add one float value and experience the magic
//    val new_primitiveDS = primitiveDS.map(_ + 1)
//    primitiveDS.show()
//    new_primitiveDS.show()

    val path = "/home/saurabh/Desktop/Spafka_RW/Spark_DSF_RW/DSF_2.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

  }
}