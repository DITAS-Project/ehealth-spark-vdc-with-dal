package com.ditas

import com.ditas.MetricsServiceTest.MetricsServiceClient
import com.ditas.ehealth.MetricsService.{GetDataSourceMetricsRequest, MetricsServiceGrpc}
import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.concurrent.ExecutionContext

@RunWith(value = classOf[JUnit4])
class MetricsServiceTest {


  @Test
  def test1 = {

    val metrics = MetricsServiceClient.getDataSourceMetrics("getMetrics", 50054, "178.22.71.88")
    val expectedMetrics = """{ "CPU": "50.0", "RAM": "1000.0", "Disk": "1000" }"""
    assertEquals(expectedMetrics, metrics)
  }


}

object MetricsServiceTest {


  object MetricsServiceClient {
    def getDataSourceMetrics(in: String, serverPort: Int, serverUrl: String): String = {
      implicit val ec = ExecutionContext.global
      val channel = ManagedChannelBuilder.forTarget(serverUrl + ":" +
        serverPort).usePlaintext(true).
        build

      val blockingStub = MetricsServiceGrpc.blockingStub(channel)
      var metrics = ""
      val request = new GetDataSourceMetricsRequest()
      try {
        val response = blockingStub.getDataSourceMetrics(request)
        metrics = response.metrics
        println(s"Metrics: ${metrics}")
      } catch {
        case ex: StatusRuntimeException =>
          val errorMessage = s"${ex.getStatus.getCode}. Query threw exception: ${ex.getStatus.getDescription} ."
          println(errorMessage)
          throw new RuntimeException(errorMessage)
      }
      metrics
    }
  }
}