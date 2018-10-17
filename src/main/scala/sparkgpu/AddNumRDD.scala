package sparkgpu

import org.apache.spark.rdd._
import org.apache.spark.{Partition, TaskContext}


/**
  * An RDD that uses GPU to add a int to every entry of the parent RDD
  *
  * @param prev the parent RDD
  * @param num The number used to add to the RDD entries
  */
class AddNumRDD(
    var prev: RDD[Int],
    num: Int)
  extends RDD[Int](prev) {

  override val partitioner = firstParent[Int].partitioner

  override def getPartitions: Array[Partition] = firstParent[Int].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    // Using CUDA to compute

    // fail to use iterator.toArray throw java.lang.NoSuchMethodError: scala.Predef$.intArrayOps([I)[I
    val arr:Array[Int] = firstParent[Int].iterator(split,context).toArray
    for ( i <- 0 to arr.size - 1)
      arr(i) += num
    arr.iterator
    // Call CUDA function here
    // ...
//    firstParent[Int].iterator(split,context).map(_ + num)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    // Don't know why
    prev = null
  }


}



