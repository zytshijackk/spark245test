package structuredstreaming.clustering

import org.apache.spark.sql.{DataFrame, SparkSession}

//source有四种：
// File source、Kafka source、Socket source (for testing) 、Rate source (for testing)
object KMeansSource {
  def getSocketSource(spark: SparkSession,host:String,port:Int): DataFrame ={
    val lines:DataFrame = spark.readStream //在流文本中每一行变成table的一行
      .format("socket")
      .option("host", host)
      .option("port", 9999)
      //      .option("includeTimestamp",true)
      .load()
    lines
  }
//  def getFlieSource(spark: SparkSession,path:String,maxFilesPerTrigger:String,latestFirst:String,fileNameOnly:String): DataFrame ={
//    var lines = spark.readStream
//      .format("file")
//      .option("path", path)
//      if(maxFilesPerTrigger!=null && !maxFilesPerTrigger.equals(""))
//        lines = lines.option("maxFilesPerTrigger",maxFilesPerTrigger)
//      if(latestFirst!=null && !latestFirst.equals(""))
//        lines = lines.option("latestFirst",latestFirst)
//      if(fileNameOnly!=null && !fileNameOnly.equals(""))
//        lines = lines.option("fileNameOnly",fileNameOnly)
//    val result: DataFrame = lines.load()
//    result
//  }
  def getTextSource(spark: SparkSession,path:String): DataFrame = {
    spark.readStream.text(path)
  }

    def getKafkaSource(): Unit ={

  }

  def getRateSource(): Unit ={

  }
}
