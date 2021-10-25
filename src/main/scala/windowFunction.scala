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
      . withColumn("max_salary", max("salary").over(byDepName))
      .withColumn("min_salary", min("salary").over(byDepName))

    agg_salary.select("depname", "max_salary", "min_salary")
      .dropDuplicates()
      .show()

  }
}
