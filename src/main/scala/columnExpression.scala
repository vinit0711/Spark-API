import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object columnExpression extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
  //  val spark = SparkSession.builder().master("local[*]").appName("week12_SparkWrite").getOrCreate()

  //  to enable hive support for meta data
  val spark = SparkSession.builder().master("local[*]").appName("week12_SparkWrite").enableHiveSupport().getOrCreate()


  val ordersDf = spark.read.format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path", "/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/orders.csv")
    .load

  ordersDf.selectExpr("order_id","order_date","concat(order_status,'_STATUS')").show(false)
}
