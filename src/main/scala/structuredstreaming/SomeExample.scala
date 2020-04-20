package structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

//无法运行的多个Example合并
object SomeExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("SomeSource")
      .getOrCreate()

    // Read text from socket
    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    socketDF.isStreaming    // Returns True for DataFrames that have streaming sources

    socketDF.printSchema

    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")


    //流上支持DataFrame / Dataset上的大多数常见操作
    import spark.implicits._ //加了这个后面df和ds互相转换就不会出错了

    val df: DataFrame = csvDF // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
    val ds: Dataset[DeviceData] = df.as[DeviceData]    // streaming Dataset with IOT device data
    // Select the devices which have signal more than 10
    df.select("device").where("signal > 10")      // using untyped APIs
    ds.rdd.filter(_.signal > 10).map(_.device)         // using typed APIs

    // Running count of the number of updates for each device type
    df.groupBy("deviceType").count()                          // using untyped API

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))    // using typed API

    //您还可以将流式DataFrame / Dataset注册为临时视图，然后在其上应用SQL命令。
    df.createOrReplaceTempView("updates")
    spark.sql("select count(*) from updates")  // returns another streaming DF
  }
}
case class DeviceData(device: String, deviceType: String, signal: Double, time: DateTime)
