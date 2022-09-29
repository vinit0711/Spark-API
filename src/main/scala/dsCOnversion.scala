import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.sql.Timestamp

case class OrdersData(order_id:Int , order_date:Timestamp ,order_customer_id:Int, order_status:String )
object dsCOnversion extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()
  val orderDf = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/orders.csv")

//  this aprk import is the session we have created , so will have to import it later only

  import spark.implicits._

  val ordersDs = orderDf.as[OrdersData]
//  as we defined a schema to it this dataset is compile time safe , we can determine error in column names at runtime only
  ordersDs.filter(x=>x.order_id <1000)
  scala.io.StdIn.readLine()
  spark.stop()
}
