import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import com.google.protobuf.wrappers.Int32Value

import scala.util.{Failure, Success, Try}
import rpcService.GpuNode.FileCreator


FileCreator.createMemoryMap("./test_dir", 1212312) match {
  case Success(_) => println("success")
  case Failure(exception) =>  {
    exception.printStackTrace()
    println("Fail")
  }
}
