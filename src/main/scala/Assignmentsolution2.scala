import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Assignmentsolution2 extends App{
  // define a schema for employee record data using a case class
  case class windowData(Country: String, Weeknum: Int, NumInvoices: Int, TotalQuantity: Int, InvoiceValue: String)
  //Create Spark Config Object
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "WEEK11_SOLUTION_2_WINDOWDATA")
  sparkConf.set("spark.master", "local[2]")
  //Create Spark Session
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._
  val windowDataDF = spark.sparkContext.textFile("G:/TRENDY~TECH/WEEK-11/Assignment_Dataset/windowdata-201021-002706.csv")
  //.toDF.map(_.split(",")).map(e => windowData(e(0), e(1).trim.toInt, e(2).trim.toInt,e(3).trim.toInt, e(4))).toDF().repartition(8)



  val windowDataDF = spark.sparkContext.textFile("G:/TRENDY~TECH/WEEK-11/Assignment_Dataset/windowdata-201021-002706.csv") //.toDF.map(_.split(",")).map(e => windowData(e(0), e(1).trim.toInt, e(2).trim.toInt,e(3).trim.toInt, e(4))).toDF().repartition(8)

//  windowDataDF.write.format("json").mode(SaveMode.Overwrite).option("path", "G:/TRENDY~TECH/WEEK-11/Assignment_Dataset/OutPut_Prb2/windowData_jsonoutput").save()
  windowDataDF.show()
  spark.stop()
  scala.io.StdIn.readLine()


//
}
