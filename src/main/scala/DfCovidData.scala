import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DfCovidData extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/orders.csv")
    df.show()
    df.createOrReplaceTempView("sqldf")
    val groupedOrdersDf=spark.sql("SELECT count(order_id), order_customer_id from sqldf Where order_customer_id > 1000 GROUP BY order_customer_id ;")
    groupedOrdersDf.show()
    scala.io.StdIn.readLine()
    spark.stop()
}