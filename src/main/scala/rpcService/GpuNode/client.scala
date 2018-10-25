package rpcService.GpuNode

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import com.google.protobuf.any.Any
import io.grpc.ManagedChannelBuilder

import scala.util.Try
;


object FileCreator {

  def createMemoryMap(path: String, bufferSize: Long): Try[Unit] = {
    Try({
      val file = new File(path)
      file.createNewFile()
      (new RandomAccessFile(file, "rw")).
        getChannel.
        map(FileChannel.MapMode.READ_WRITE, 0, bufferSize)
    })
  }

  def execute(path: String, bufferSize: Long): Try[String] = {
    Try({
      createMemoryMap(path, bufferSize)
      val channel = ManagedChannelBuilder.forAddress("0.0.0.0", 5000).usePlaintext(true).build()
      val request = rpcService.GpuNode.createFileInput(bufferSize, path)
      val stub = rpcService.GpuNode.GpuNodeGrpc.blockingStub(channel)
      stub.
        createFile(request).
        getFieldByNumber(1).
        asInstanceOf[String]
    })
  }
}

object KernelCaller {
  def execute(fpath: String, func: String, param: Long): Try[(String, Int)] = {
    Try({
      val channel = ManagedChannelBuilder.forAddress("0.0.0.0", 5000).usePlaintext(true).build()
      val request = rpcService.GpuNode.kernelCallInput(fpath, func, param)
      val stub = rpcService.GpuNode.GpuNodeGrpc.blockingStub(channel)
      val response = stub.callKernel(request)
      (response.getFieldByNumber(1).asInstanceOf[String], response.getFieldByNumber(2).asInstanceOf[Int])
    })
  }
}

object Test extends App {
    println(FileCreator.execute("/home/francis/memoryBuffer", 2000))
//  println(KernelCaller.execute("tt", "tt", 123123))
}
