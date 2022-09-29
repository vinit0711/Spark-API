import org.apache.spark.sql.{SaveMode, SparkSession}

object week12_SparkWrite extends App {
//  Logger.getLogger("org").setLevel(Level.ERROR)
//  val spark = SparkSession.builder().master("local[*]").appName("week12_SparkWrite").getOrCreate()

//  to enable hive support for meta data
val spark = SparkSession.builder().master("local[*]").appName("week12_SparkWrite").enableHiveSupport().getOrCreate()


  val ordersDf = spark.read.format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path", "/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/orders.csv")
    .load

  spark.sql("create database if not exists retail")


//  writing to db without mentioning db , will save it to default db
  ordersDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4,"order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("retail.orders2")



}
