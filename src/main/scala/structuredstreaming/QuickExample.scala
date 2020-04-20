package structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//+------------------------------------------+----+-----+
//|window                                    |word|count|
//+------------------------------------------+----+-----+
//|[2020-04-15 19:18:00, 2020-04-15 19:22:00]|he  |2    |
//|[2020-04-15 19:16:00, 2020-04-15 19:20:00]|he  |2    |
//+------------------------------------------+----+-----+
object QuickExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
        .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines:DataFrame = spark.readStream //在流文本中每一行变成table的一行
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp",true)//添加自带的时间戳
      .load()

    import org.apache.spark.sql.functions._
    // Split the lines into words
//    val words:Dataset[String] = lines.as[String].flatMap(_.split(" "))
    val words = lines
      .as[(String,Timestamp)]//转成Dataset
      .flatMap{
      case (words,ts)=>words.split("\\W+").map((_,ts))
      }
      .toDF("word","ts")//转成DataFrame并设置别名
      .groupBy(
        window($"ts","4 minutes","2 minutes"),
        $"word"
      )//根据时间戳和word进行分区
      .count()
    // Generate running word count
//    val wordCounts:DataFrame = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = words.writeStream
//      .outputMode("append")//后面不会发生变化才可以（没聚合=update，有聚合一定要有watermark）
      ////      .outputMode("complete")//全输出。必须有聚合
      .outputMode("update")//只输出变化的部分
      .format("console")
      .option("truncate",false)//不省略显示
      .start()

    query.awaitTermination()
  }
}
