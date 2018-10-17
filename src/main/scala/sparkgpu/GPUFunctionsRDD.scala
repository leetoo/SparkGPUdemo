package sparkgpu

import org.apache.spark.rdd.RDD

class GPUFunctionsRDD[U](self: RDD[U]) {


  /** Add a number to every entry of a RDD[Int] with GPU
    *
    * @param num The number used to add to the RDD entries
    * @return An AddNumRDD
    */
  def addNum(num: Int): RDD[Int] = {
      new AddNumRDD(self.asInstanceOf[RDD[Int]],num)
  }

}
