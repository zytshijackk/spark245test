package structuredstreaming.clustering.utils

//欧氏距离
object EuclideanDistanceMeasure{

  def distance(point: Array[Double], center: Array[Double]): Double = {
    //    for(x<-point){
    //      System.out.println(x)
    //    }
    var a = 0.0
    for (i <- 0 to center.size - 1) {

      a += (center(i) - point(i)) * (center(i) - point(i)) //差的平方

    }

    math.sqrt(a)
  }

  /**
   * 返回最近的Center对应的ID和距离
   *
   * @param centers
   * @param point
   * @return
   */
  def findClosest(
                    point: Array[Double],
//                    centers: Array[List[Double]]
                    centers: Array[Array[Double]]
                  ): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      val currentDistance = distance(point, center)
      if (currentDistance < bestDistance) {
        bestDistance = currentDistance
        bestIndex = i
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  def addDenseVector(_2: Array[Double], _21: Array[Double]):Array[Double] = {
    BLAS.axpy(1,_2,_21) //y += a*x
    _21
  }
  def divDenseVector(_2: Array[Double], _21: Long):Array[Double] = {
    System.out.println(_2)
    System.out.println(_21)
    val a = BLAS.scal( 1/_21 , _2 ) // x= a*x
//    System.out.println(a)
    _2
  }
}