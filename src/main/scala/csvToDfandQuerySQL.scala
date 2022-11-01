import org.apache.spark.sql.SparkSession

object csvToDfandQuerySQL extends App {

  val spark = SparkSession.builder().master("local[*]").appName("csvToDfandQuerySQL").getOrCreate()
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/order_data.csv")

//  query using string Expression
//  df.selectExpr("count(*) as RowCount","sum(Quantity)as TotalQuantity").show()

//  For pure sql create temp view

  df.createOrReplaceTempView("sales")
  spark.sql("select count(*) from sales").show()

//  df.printSchema()
  df.show()

}
