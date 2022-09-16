import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DfCovidData extends App {
    val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/infinity//Documents/Spark/Examples/covidusa/*.csv")
    //    df.show()
    df.createOrReplaceTempView("sqldf")
    //Question : Deaths state wise
    //val sqldfusa=spark.sql("SELECT state, sum(death)/sum(positive) FROM sqldf GROUP BY state")
    //    sqldfusa.show()
    //    sqldfusa.write.csv("/home/infinity//Documents/Spark/Examples/covidusa/output.csv")
    //Question : Deaths month wise
    val sqldfusa=spark.sql("SELECT EXTRACT(month from date) as month, sum(death)" +
      "from sqldf group by month ORDER BY month;")
    sqldfusa.show()
}