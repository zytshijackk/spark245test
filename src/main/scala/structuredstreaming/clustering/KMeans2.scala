package structuredstreaming.clustering

import org.apache.spark.sql.Dataset
import structuredstreaming.clustering.utils.{BLAS, EuclideanDistanceMeasure}

class KMeans2(
                //               var k: Int,
                var maxIterations: Int,
                var centroids:Dataset[Array[Double]]
              ) extends Serializable {
  def train(points:Dataset[Array[Double]]): Dataset[Array[Double]] = {
    val ssc = points.sparkSession
    import ssc.implicits._
    val sc = ssc.sparkContext

    var iteration = 0
//    while (iteration < maxIterations) {
      val center  = centroids.collect() //collect两次就出错了
      val bcCenters = sc.broadcast(center)
      val newCentroids = points
        .map(point => {
          var minDistance: Double = Double.MaxValue
          var closestCentroidId: Int = -1
          //返回的是（中心下标，点信息）
          for (i <- bcCenters.value.indices) {
            val arr = bcCenters.value
            val distance = EuclideanDistanceMeasure.distance(point, arr(i))
            if (distance < minDistance) {
              minDistance = distance
              closestCentroidId = i
            }
          }
          (closestCentroidId, point)
        })
        .map ( x => (x._1, (x._2, 1L)) )
        .groupByKey(_._1).reduceGroups{
        (x1,x2)=>(x1._1,
          ({
            BLAS.axpy(1.0,x1._2._1,x2._2._1)
            x2._2._1
          },x1._2._2+x2._2._2))
      }.map{
        x=>{
          BLAS.scal(1.0/x._2._2._2,x._2._2._1)
          x._2._2._1
        }
      }

      iteration += 1
      centroids = newCentroids
//      bcCenters.destroy()
//    }
    centroids
//    newCentroids
  }

  //  def train2(data:Dataset[Array[Double]]): RDD[(Int, (Vector, Double))]  ={
//    val ssc = data.sparkSession
//    import ssc.implicits._
//    val sc = ssc.sparkContext
//    var iteration = 0
////    var currentCentroids = centroids
//    val costAccum = sc.doubleAccumulator
//    val countAccum = sc.longAccumulator
//    val bcCenters = sc.broadcast(centroids)
//    val collected: RDD[(Int, (Vector, Double))] = data.mapPartitions{ points=>
//      val thisCenters = bcCenters.value
//      val sums = Array.fill(thisCenters.length)(Vectors.zeros(thisCenters.head.size))
//      // clusterWeightSum is needed to calculate cluster center
//      // cluster center =
//      //     sample1 * weight1/clusterWeightSum + sample2 * weight2/clusterWeightSum + ...
//      val clusterWeightSum = Array.ofDim[Double](thisCenters.size)
//      points.foreach(point=>{
//        val (bestCenter, cost) = EuclideanDistanceMeasure.findClosest(point,thisCenters)
//        costAccum.add(cost * 1.0)
//        countAccum.add(1)
//        BLAS.axpy(1.0,point,sums(bestCenter).toArray)
//        clusterWeightSum(bestCenter) += 1.0
//      })
//      clusterWeightSum.indices.filter(clusterWeightSum(_) > 0)
//        .map(j => (j, (sums(j), clusterWeightSum(j)))).iterator
//    }.rdd.reduceByKey { case ((sum1, clusterWeightSum1), (sum2, clusterWeightSum2)) =>
//      BLAS.axpy(1.0, sum2.toArray, sum1.toArray)
//      (sum1, clusterWeightSum1 + clusterWeightSum2)
//    }
//    collected
//  }


}
