package com.ditas

import com.ditas.ehealth.EHealthService.{EHealthQueryRequest, EHealthQueryServiceGrpc}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.testing.GrpcCleanupRule
import org.junit.{Rule, Test}
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(value = classOf[JUnit4])
class EHealthServiceTest {

  @Rule
  val grpcCleanup: GrpcCleanupRule = new GrpcCleanupRule

  @Test
  def test1 = {
        val serverName = InProcessServerBuilder.generateName()
    //     builder.addService(EHealthQueryServiceGrpc.
        //      bindService(new EhealthServer.EHealthQueryServiceImpl, executionContext))
            grpcCleanup.register(InProcessServerBuilder.forName(serverName).directExecutor()
              .addService(new EhealthServer.EHealthQueryServiceImpl).build().start())

        val blockingStub = EHealthQueryServiceGrpc.blockingStub(grpcCleanup.register((InProcessChannelBuilder.forName(serverName).directExecutor().build())))

        val reply = blockingStub.query(new EHealthQueryRequest())
  }


}