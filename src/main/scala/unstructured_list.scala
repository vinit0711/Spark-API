
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

object unstructured_list extends App {

  //  to enable hive support for meta data
  val spark = SparkSession.builder().master("local[*]").appName("unstructured_list").getOrCreate()
  val myList= List(
    (1,"2013-07-25", 11599, "CLOSED"),
    (2,"2014-07-25", 256, "PENDING PAYMENT"),
    (3,"2015-07-25", 156, "CLOSED"),
    (4,"2013-07-25", 11599, "COMPLETE"),
    (5,"2017-07-25", 8827, "CLOSED"))

  val ordersDf=spark.createDataFrame(myList).toDF("orderid","orderdate","customerid","status")

//  if we want to modfy col

  val newOrdersDf=ordersDf.withColumn("orderdate",unix_timestamp(col("orderdate").cast(DateType)))

//  add a new column and make sure it has only unique ids

  val uniqueOrdersDf=newOrdersDf.withColumn("newid",monotonically_increasing_id)

//  drop duplicated considering orderdate and customer id

  val uniqueOrdersDf2=uniqueOrdersDf.dropDuplicates("orderdate","customerid")
  uniqueOrdersDf2.printSchema()
  uniqueOrdersDf2.show()



}
