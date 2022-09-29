import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AssignmentWindow extends App {
//  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()

  val windowSchema = StructType(List(
    StructField("country", StringType),
    StructField("weeknum", IntegerType),
    StructField("numinvoices", IntegerType),
    StructField("totalquantity", StringType),
    StructField("invoicevalue", FloatType)
  ))
  val orderDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(windowSchema)
    .option("path", "/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/windowdata.csv")
    .load
  orderDf.printSchema()
  orderDf.show(false)


//  orderDf.write
//    .partitionBy("country", "weeknum")
//    .mode(SaveMode.Overwrite)
//    .parquet("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/orderDf")

  orderDf.write.partitionBy("country")
    .format("avro").save("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/orderDf/avro_out")

  //
//  scala.io.StdIn.readLine()

}
