import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object joinsExample extends App {
  val spark = SparkSession.builder().master("local[*]").appName("csvToDfandQuerySQL").getOrCreate()

//  reading orders data
  val ordersdf = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true").
    option("header","true")
    .load("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/orders.csv")


//  we want to join cust_id from both table , to avoid conflicts we will rename the col name

//  val ordersdfNew=ordersdf.withColumnRenamed("customer_id","cust_id")


//  reading customers data
  val customersdf = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("header","true")
  .load("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/customers.csv")

//  join condition
  val joincondition =ordersdf.col("order_customer_id")===customersdf.col("customer_id")

//  type of join
  val jointype="outer"
//  joining 2 dataframes
  ordersdf.join(customersdf,joincondition,jointype)
    .sort("order_id")
    .show()

//  If we run above query it will give some null values in order id , now how to hanlde null values .if we want to replace null with -1
  ordersdf.join(customersdf, joincondition, jointype)
    .sort("order_id")
    .withColumn("order_id",expr("coalesce(order_id,-1)"))
    .show()
  scala.io.StdIn.readLine()
  spark.stop()

//  In the above example shuffle and sort join took place , Now if we want to avoid this and perform broadcast join
//  ordersdf.join(broadcast(customersdf), joincondition, jointype)
//    .sort("order_id")
//    .show()
}
