package structuredstreaming.clustering.utils

object BLAS {
  def axpy(a:Double,x:Array[Double],y:Array[Double]): Unit ={ //y += a*x
    for(i <- 0 to y.size-1){
      y(i)+=a*x(i)
    }
  }

  def scal(a:Double,x:Array[Double]): Unit ={ // x= a*x
    for(i <- 0 to x.size-1){
      x(i) = a*x(i)
    }
  }
}
