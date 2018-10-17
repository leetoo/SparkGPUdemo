package sparkgpu

import org.apache.spark.rdd._
object implicitFunc {
  implicit def rddToGPUFunctionsRDD[U](rdd: RDD[U]): GPUFunctionsRDD[U] = {
    new GPUFunctionsRDD[U](rdd)
  }
}
