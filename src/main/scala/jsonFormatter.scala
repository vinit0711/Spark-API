import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object jsonFormatter extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()
  val orderDf = spark.read.format("json")
    .option("path", "/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/players.json")
    .option("mode", "FAILFAST")
    .load
  orderDf.printSchema
  orderDf.show(false)


//  Json Formatter has read modes

//  1. PERMISSIVE
//  2.DROPMALFORMED
//  3.FAILFAST
//
  scala.io.StdIn.readLine()
  spark.stop()
}
