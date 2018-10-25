package rpcService.GpuNode

import _root_.akka.NotUsed
import _root_.akka.stream.Materializer
import _root_.akka.stream.scaladsl.{ Flow, Sink, Source }
import _root_.com.google.protobuf.Descriptors.ServiceDescriptor
import _root_.com.trueaccord.scalapb.grpc.{ AbstractService, ConcreteProtoFileDescriptorSupplier, Grpc, Marshaller, ServiceCompanion }
import _root_.grpc.akkastreams.GrpcAkkaStreams._
import _root_.io.grpc.{ CallOptions, Channel, MethodDescriptor, ServerServiceDefinition }
import _root_.io.grpc.stub.{ AbstractStub, ClientCalls, ServerCalls, StreamObserver }
import _root_.org.reactivestreams.{ Publisher, Subscriber }
import _root_.scala.concurrent.Await
import _root_.scala.concurrent.duration._
import _root_.scala.util.{ Failure, Success }

object GpuNodeGrpcAkkaStream {
  val METHOD_CREATE_FILE: MethodDescriptor[rpcService.GpuNode.createFileInput, rpcService.GpuNode.createFileOutput] =
    MethodDescriptor.newBuilder()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(MethodDescriptor.generateFullMethodName("rpcService.GpuNode", "CreateFile"))
      .setRequestMarshaller(new Marshaller(rpcService.GpuNode.createFileInput))
      .setResponseMarshaller(new Marshaller(rpcService.GpuNode.createFileOutput))
      .build()
  val METHOD_CALL_KERNEL: MethodDescriptor[rpcService.GpuNode.kernelCallInput, rpcService.GpuNode.kernelCallOutput] =
    MethodDescriptor.newBuilder()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(MethodDescriptor.generateFullMethodName("rpcService.GpuNode", "CallKernel"))
      .setRequestMarshaller(new Marshaller(rpcService.GpuNode.kernelCallInput))
      .setResponseMarshaller(new Marshaller(rpcService.GpuNode.kernelCallOutput))
      .build()
  
  val SERVICE: _root_.io.grpc.ServiceDescriptor = _root_.io.grpc.ServiceDescriptor.newBuilder("rpcService.GpuNode")
    .setSchemaDescriptor(new ConcreteProtoFileDescriptorSupplier(rpcService.GpuNode.GpuNodeProto.javaDescriptor))
    .addMethod(METHOD_CREATE_FILE)
    .addMethod(METHOD_CALL_KERNEL)
    .build()
  
  trait GpuNode extends AbstractService {
    override def serviceCompanion = GpuNode
    def createFile: Flow[rpcService.GpuNode.createFileInput, rpcService.GpuNode.createFileOutput, NotUsed]
    def callKernel: Flow[rpcService.GpuNode.kernelCallInput, rpcService.GpuNode.kernelCallOutput, NotUsed]
  }
  
  object GpuNode extends ServiceCompanion[GpuNode] {
    implicit def serviceCompanion: ServiceCompanion[GpuNode] = this
    def javaDescriptor: ServiceDescriptor =
      rpcService.GpuNode.GpuNodeProto.javaDescriptor.getServices().get(0)
  }
  
  class GpuNodeStub(
    channel: Channel,
    options: CallOptions = CallOptions.DEFAULT
  ) extends AbstractStub[GpuNodeStub](channel, options) with GpuNode {
    override def createFile: Flow[rpcService.GpuNode.createFileInput, rpcService.GpuNode.createFileOutput, NotUsed] =
      Flow[rpcService.GpuNode.createFileInput].flatMapConcat(request =>
        Source.fromFuture(
          Grpc.guavaFuture2ScalaFuture(
            ClientCalls.futureUnaryCall(channel.newCall(METHOD_CREATE_FILE, options), request)
          )
        )
      )
    override def callKernel: Flow[rpcService.GpuNode.kernelCallInput, rpcService.GpuNode.kernelCallOutput, NotUsed] =
      Flow[rpcService.GpuNode.kernelCallInput].flatMapConcat(request =>
        Source.fromFuture(
          Grpc.guavaFuture2ScalaFuture(
            ClientCalls.futureUnaryCall(channel.newCall(METHOD_CALL_KERNEL, options), request)
          )
        )
      )
    override def build(channel: Channel, options: CallOptions): GpuNodeStub =
      new GpuNodeStub(channel, options)
  }
  
  def bindService(serviceImpl: GpuNode)(implicit mat: Materializer): ServerServiceDefinition =
    ServerServiceDefinition
      .builder("rpcService.GpuNode")
      .addMethod(
        METHOD_CREATE_FILE,
        ServerCalls.asyncUnaryCall(
          new ServerCalls.UnaryMethod[rpcService.GpuNode.createFileInput, rpcService.GpuNode.createFileOutput] {
            override def invoke(request: rpcService.GpuNode.createFileInput, responseObserver: StreamObserver[rpcService.GpuNode.createFileOutput]) =
              Source
                .single(request)
                .via(serviceImpl.createFile)
                .runForeach(responseObserver.onNext)
                .onComplete {
                  case Success(_) => responseObserver.onCompleted()
                  case Failure(t) => responseObserver.onError(t)
                }(mat.executionContext)
          }
        )
      )
      .addMethod(
        METHOD_CALL_KERNEL,
        ServerCalls.asyncUnaryCall(
          new ServerCalls.UnaryMethod[rpcService.GpuNode.kernelCallInput, rpcService.GpuNode.kernelCallOutput] {
            override def invoke(request: rpcService.GpuNode.kernelCallInput, responseObserver: StreamObserver[rpcService.GpuNode.kernelCallOutput]) =
              Source
                .single(request)
                .via(serviceImpl.callKernel)
                .runForeach(responseObserver.onNext)
                .onComplete {
                  case Success(_) => responseObserver.onCompleted()
                  case Failure(t) => responseObserver.onError(t)
                }(mat.executionContext)
          }
        )
      )
      .build()
  
  def stub(channel: Channel): GpuNodeStub = new GpuNodeStub(channel)
  
  def javaDescriptor: ServiceDescriptor =
    rpcService.GpuNode.GpuNodeProto.javaDescriptor.getServices().get(0)
}
