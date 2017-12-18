package sparksql


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by maheshtikky on 12/17/17.
  */
object NOAA extends App{

val spark = SparkSession.builder().appName("Noaa Data").master("local[*]").config("spark.driver.host","localhost").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val tschema = StructType(Array(
    StructField("sid",StringType),
    StructField("date",DateType),
    StructField("mtype",StringType),
    StructField("value",DoubleType)))

  val dataTfile = spark.read.schema(tschema).option("dateFormat","yyyyMMdd").csv("/Users/maheshtikky/Desktop/2017.csv")
  //dataTfile.show(10)
  //dataTfile.schema.printTreeString()


  val tmaxfilter = dataTfile.filter($"mtype" === "TMAX").limit(1000).drop("mtype").withColumnRenamed("value","Tmax")
  val tminfilter = dataTfile.filter('mtype === "TMIN").limit(1000).drop("mtype").withColumnRenamed("value","Tmin")
  val joindata = tmaxfilter.join(tminfilter, Seq("sid","date"))
  val averagedata = joindata.select('sid, 'date, ('tmax + 'tmin)/2 )
  averagedata.show(14)






  spark.close()
}
