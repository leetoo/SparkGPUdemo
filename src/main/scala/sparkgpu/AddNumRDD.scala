package sparkgpu

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.rdd._
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{Partition, TaskContext}
import rpcService.GpuNode._

import scala.io.Source
import scala.util._

/**
  * An RDD that uses GPU to add a int to every entry of the parent RDD
  *
  * @param prev the parent RDD
  * @param num  The number used to add to the RDD entries
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
    var arr: Array[Int] = firstParent[Int].iterator(split, context).toArray
    // write file
    // rpc
    val block = RDDBlockId(this.id, split.index)
    val filePath = "/home/francis/Spark-temp/" + block.name
    val filec = FileCreator.execute(filePath, arr.size * 4) match {
      case Success(msg) => {
        println(msg)
      }
      case Failure(exception) => {
        exception.printStackTrace()
      }
    }
    //copy rdd data
    var out = new FileOutputStream(filePath)
    for(i <- arr) {
      out.write(i & 0x000000FF)
      out.write((i & 0x0000FF00) >> 8)
      out.write((i & 0x00FF0000) >> 16)
      out.write((i & 0xFF000000) >> 24)
    }
    //out.flush()
    //out.close()
    // Call CUDA function here
    // read file
    val modulePath = "/home/francis/Spark-temp/scala_grpc_demo/gpuServer/cpp/addn.ptx"
    KernelCaller.execute(modulePath, "add_n", num) match {
      case Success((resultPath, resultSize)) => {
        println(resultPath)
        println(resultSize)
        val in = new FileInputStream(resultPath)
        val buf = new Array[Byte](resultSize)
        in.read(buf)
        for(i <- 0 to resultSize/4 -1) {
          arr(i) =
                (buf(i * 4) & 0xFF) +
                ((buf(i * 4 + 1) & 0xFF) << 8) +
                ((buf(i * 4 + 2) & 0xFF) << 16) +
                ((buf(i * 4 + 3) & 0xFF) << 24)
          println(arr(i))
        }
//        buf.foreach(println)
//        arr.foreach(println)
        arr.iterator
      }
      case Failure(exception) => {
        firstParent[Int].iterator(split, context)
      }
    }
  }

  //
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    // Don't know why
    prev = null
  }


}



