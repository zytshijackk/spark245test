package structuredstreaming.clustering

import java.sql.Timestamp

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KMeansTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = KMeansSource.getSocketSource(spark,"localhost",9999)
//    val lines: DataFrame = KMeansSource.getTextSource(spark,"/Users/zytshijack/IdeaProjects/spark245test/src/main/resources/kmeans/")
    // Split the lines into words
//    val words:Dataset[String] = lines.as[String].flatMap(_.split(" "))
    val points: Dataset[Array[Double]] = lines.as[String].map(_.split(",").map(x=>x.toDouble))
    val center: Dataset[Array[Double]] = spark.sparkContext.parallelize(Array(Array(1.0,1.0),Array(1.0,2.0))).toDS()
    val kmeans = new KMeans2(2,center)
    val re: Dataset[Array[Double]] = kmeans.train(points)
//    val kmeans2 =  new KMeans2(2,re)
//    val re2 = kmeans2.train(points)

//    val kmeans = new KMeans(1,Array(Array(1.0,1.0),Array(1.0,2.0)))
//    var re: Dataset[Array[Double]] = kmeans.train(points)

    val query: StreamingQuery = re.writeStream
//      .outputMode("append")//后面不会发生变化才可以（没聚合=update，有聚合一定要有watermark）
      .outputMode("complete")//全输出。必须有聚合
//      .outputMode("update")//只输出变化的部分
      .format("console")
//      .trigger(Trigger.Once())
      .trigger(Trigger.ProcessingTime(1000*5))
      .start()
    query.awaitTermination()

//    query.lastProgress
    //Dataset[Vector]好像不支持，所以转成了rdd
//    val words:Dataset[String] = lines.as[String]
//    val points: RDD[Vector] = words.rdd.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
//




  }
}
