import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StructFieldExample extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()


  val ordersSchema = StructType(List(
    StructField("orderid", IntegerType),
    StructField("orderdate", TimestampType),
    StructField("customerid", IntegerType),
    StructField("status", StringType)

  ))
  val orderDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(ordersSchema)
    .option("path", "/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/orders.csv")
    .load
  orderDf.printSchema()
  orderDf.show(false)


  //
  scala.io.StdIn.readLine()
  spark.stop()
}
