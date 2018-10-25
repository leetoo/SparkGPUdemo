package rpcService.GpuNode

import _root_.com.google.protobuf.Descriptors.ServiceDescriptor
import _root_.com.trueaccord.scalapb.grpc.{ AbstractService, ConcreteProtoFileDescriptorSupplier, Marshaller, ServiceCompanion }
import _root_.io.grpc.{ CallOptions, Channel, MethodDescriptor, ServerServiceDefinition }
import _root_.grpcmonix.GrpcMonix._
import _root_.io.grpc.stub.{ AbstractStub, ClientCalls, ServerCalls, StreamObserver }
import _root_.monix.eval.Task
import _root_.monix.execution.{ Cancelable, Scheduler }
import _root_.monix.reactive.Observable
import _root_.org.reactivestreams.{ Publisher => PublisherR, Subscriber => SubscriberR }

object GpuNodeGrpcMonix {
  
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
    def createFile(request: rpcService.GpuNode.createFileInput): Task[rpcService.GpuNode.createFileOutput]
    def callKernel(request: rpcService.GpuNode.kernelCallInput): Task[rpcService.GpuNode.kernelCallOutput]
  }
  
  object GpuNode extends ServiceCompanion[GpuNode] {
    implicit def serviceCompanion: ServiceCompanion[GpuNode] = this
    def javaDescriptor: ServiceDescriptor = rpcService.GpuNode.GpuNodeProto.javaDescriptor.getServices().get(0)
  }
  
  class GpuNodeStub(
    channel: Channel,
    options: CallOptions = CallOptions.DEFAULT
  ) extends AbstractStub[GpuNodeStub](channel, options) with GpuNode {
    override def createFile(request: rpcService.GpuNode.createFileInput): Task[rpcService.GpuNode.createFileOutput] = 
      guavaFutureToMonixTask(
        ClientCalls.futureUnaryCall(channel.newCall(METHOD_CREATE_FILE, options), request)
      )
    override def callKernel(request: rpcService.GpuNode.kernelCallInput): Task[rpcService.GpuNode.kernelCallOutput] = 
      guavaFutureToMonixTask(
        ClientCalls.futureUnaryCall(channel.newCall(METHOD_CALL_KERNEL, options), request)
      )
    override def build(channel: Channel, options: CallOptions): GpuNodeStub = 
      new GpuNodeStub(channel, options)
  }
  
  def bindService(serviceImpl: GpuNode, scheduler: Scheduler): ServerServiceDefinition = 
    ServerServiceDefinition
      .builder("rpcService.GpuNode")
      .addMethod(
        METHOD_CREATE_FILE,
        ServerCalls.asyncUnaryCall(
          new ServerCalls.UnaryMethod[rpcService.GpuNode.createFileInput, rpcService.GpuNode.createFileOutput] {
            override def invoke(request: rpcService.GpuNode.createFileInput, observer: StreamObserver[rpcService.GpuNode.createFileOutput]): Unit =
              serviceImpl.createFile(request).runAsync(grpcObserverToMonixCallback(observer))(scheduler)
          }
        )
      )
      .addMethod(
        METHOD_CALL_KERNEL,
        ServerCalls.asyncUnaryCall(
          new ServerCalls.UnaryMethod[rpcService.GpuNode.kernelCallInput, rpcService.GpuNode.kernelCallOutput] {
            override def invoke(request: rpcService.GpuNode.kernelCallInput, observer: StreamObserver[rpcService.GpuNode.kernelCallOutput]): Unit =
              serviceImpl.callKernel(request).runAsync(grpcObserverToMonixCallback(observer))(scheduler)
          }
        )
      )
      .build()
  
  def stub(channel: Channel): GpuNodeStub = new GpuNodeStub(channel)
  
  def javaDescriptor: ServiceDescriptor = 
    rpcService.GpuNode.GpuNodeProto.javaDescriptor.getServices().get(0)
}
