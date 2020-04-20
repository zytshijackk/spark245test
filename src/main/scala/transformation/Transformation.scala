package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    //map
//    val rdd = sc.parallelize(1 to 10)  //创建RDD
//    val map = rdd.map(_*2)             //对RDD中的每个元素都乘于2
//    map.foreach(x => print(x+" "))

    //flatMap
//    val rdd: RDD[Int] = sc.parallelize(1 to 5)
//    val fm: Array[Int] = rdd.flatMap(x => (1 to x)).collect()
//    fm.foreach( x => print(x + " "))

    //distinct 去重
//    val list = List(1,1,2,5,2,9,6,1)
//    val distinctRDD = sc.parallelize(list)
//    val unionRDD = distinctRDD.distinct()
//    unionRDD.collect.foreach(x => print(x + " "))

    //cartesian 笛卡尔积
//    val rdd1 = sc.parallelize(1 to 3)
//    val rdd2 = sc.parallelize(2 to 5)
//    val cartesianRDD = rdd1.cartesian(rdd2)
//    cartesianRDD.foreach(x => println(x + " "))

    //glom 将RDD的每个分区中的类型为T的元素转换换数组Array[T]
//    val rdd = sc.parallelize(1 to 16,4)
//    val glomRDD = rdd.glom() //RDD[Array[T]]
//    glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))

    //intersection(otherDataset):返回两个RDD的交集
//    val rdd1 = sc.parallelize(1 to 3)
//    val rdd2 = sc.parallelize(3 to 5)
//    val unionRDD = rdd1.intersection(rdd2)
//    unionRDD.collect.foreach(x => print(x + " "))

    //union(ortherDataset):将两个RDD中的数据集进行合并，最终返回两个RDD的并集，若RDD中存在相同的元素也不会去重
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(3 to 5)
    val unionRDD = rdd1.union(rdd2)
    unionRDD.collect.foreach(x => print(x + " "))

    sc.stop()
  }
}
