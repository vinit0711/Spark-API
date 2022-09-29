import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
Plan

1.Problem - We have a dataset which is having unstructured data . We have do some operations on it . But as it is unstructured we cannot use higher level constructs such as Df and DS on it . So we will initially use low level cosntruct such as rdd . parse the data and convert it into a structured data . Now once we give structure we can convert it to DS/DF

 */
object unstructuredDataHandling {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("spark Sql").getOrCreate()

//  now spark session is umbrella we can have spark contest inside spark contest
  val lines= spark.sparkContext.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/order_unstructured.txt")

  val myRegexExprssion="""(\S+)\t(\S+) \t(\S+)\,(\S+)""".r
  case class Orders(order_id:Int,customer_id:Int,order_status:String)
  def parser (line: String) ={
    line match{
      case myRegexExprssion(order_id,data,customer_id,order_status)=>
        Orders(order_id.toInt,customer_id.toInt,order_status)
    }
  }

  import spark.implicits._
  val ordersDS= lines.map(parser).toDS().cache()

  ordersDS.printSchema()
}
