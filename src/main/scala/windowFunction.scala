import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class Salary(depName: String, empNo: Long, Salary: Long)

object windowFunction {

  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("windowFunction")
      .config("spark.master","local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import  spark.implicits._

    val empsalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)).toDF()

    empsalary.show()

    val byDepName = Window.partitionBy("depName")

    val agg_salary = empsalary
      .withColumn("max_salary", max("salary").over(byDepName))
      .withColumn("min_salary", min("salary").over(byDepName))

    agg_salary.select("depname", "max_salary", "min_salary")
      .dropDuplicates()
      .show()

//    *** Activate this code before running below application ***
//    val winSpec = Window.partitionBy("depName").orderBy(desc("salary"))

    // *** there are some other functions i.e row_number(), percent_rank() & ntile etc. we can use ***
//    val rank_df = empsalary.withColumn("rank", rank().over(winSpec))
//    rank_df.show()
//
//    val dense_rank_df = empsalary.withColumn("dense_rank", dense_rank().over(winSpec))
//    dense_rank_df.show()
//
//
//    val row_num_df = empsalary.withColumn("row_number", row_number().over(winSpec))
//    row_num_df.show()
//
//
//    val percent_rank_df = empsalary.withColumn("percent_rank", percent_rank().over(winSpec))
//    percent_rank_df.show()
//
//    val ntile_df = empsalary.withColumn("ntile", ntile(3).over(winSpec))
//    ntile_df.show()


//      *** Window analytical functions ***

//    val cume_dist_df = empsalary.withColumn("cume_dist", cume_dist().over(winSpec))
//    cume_dist_df.show()
//
//    val lag_df = empsalary.withColumn("lag", lag("salary", 2).over(winSpec))
//    lag_df.show()
//
//    val lead_df = empsalary.withColumn("lead", lead("salary", 2).over(winSpec))
//    lead_df.show()

//    *** Customn Window Function ***

//      val winSpec = Window.partitionBy("depName")
//        .orderBy("salary")
//        .rangeBetween(100L, 300L) // Window.current, Window.unboundedPreceding & Window.unboundedFollowing can be used accordingly
//
//      val range_between_df = empsalary
//        .withColumn("max_salary", max("salary")
//          .over(winSpec))
//
//      range_between_df.show()

    val winSpec = Window.partitionBy("depName")
      .orderBy("salary").rowsBetween(-1,1) // create window by using one previous & one ahead column

    val rows_between_df = empsalary.withColumn("max_salary", max("salary").over(winSpec))
    rows_between_df.show()

  }
}
