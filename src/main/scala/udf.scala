import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

object udf extends App {

  def ageCheck(age: Int): String = {
    if (age > 18) "Y" else "N"
  }


  Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("week12_SparkWrite").getOrCreate()

  val customerDf = spark.read.format("csv")
    .option("inferschema",true)
    .option("path", "/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api-2/dataset1.csv")
    .load

  /*
  Problem  Statement : Add a fourth col with if cistomer age >18 aadd Y else N
  Plan earlier we did using rdd now we will do using higher level construct >>for that we have to use udf
   */

  import spark.implicits._

  val structuredDf = customerDf.toDF("name","age","city")
  val parseAfeFunction = functions.udf(ageCheck( _:Int):String)

//  if want to add new col use with Column
//  val df2= structuredDf.withColumn("Adult",parseAfeFunction(col("age")))

  spark.udf.register("parseAfeFunction",(x:Int)=>{if (x>18) "Y" else "N" })

//  val df3=structuredDf.withColumn("adult",expr("parseAfeFunction(age)"))

  structuredDf.createOrReplaceTempView("customertable")

  spark.sql("select name ,age,city,parseAfeFunction(age) as adult from customertable").show()
//  df3.show()


}
