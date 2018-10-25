package rpcService.GpuNode

import _root_.com.trueaccord.scalapb.GeneratedMessage
import _root_.com.trueaccord.scalapb.json.JsonFormat
import _root_.grpcgateway.handlers._
import _root_.io.grpc._
import _root_.io.netty.handler.codec.http.{HttpMethod, QueryStringDecoder}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import com.trueaccord.scalapb.json.JsonFormatException
import scala.util._

class GpuNodeHandler(channel: ManagedChannel)(implicit ec: ExecutionContext)
  extends GrpcGatewayHandler(channel)(ec) {
  override val name: String = "GpuNode"
  private val stub = GpuNodeGrpc.stub(channel)
  
  override def supportsCall(method: HttpMethod, uri: String): Boolean = {
    val queryString = new QueryStringDecoder(uri)
    (method.name, queryString.path) match {
      case _ => false
    }
  }
  
  override def unaryCall(method: HttpMethod, uri: String, body: String): Future[GeneratedMessage] = {
    val queryString = new QueryStringDecoder(uri)
    (method.name, queryString.path) match {
      case (methodName, path) => 
        Future.failed(InvalidArgument(s"No route defined for $methodName($path)"))
    }
  }
}

