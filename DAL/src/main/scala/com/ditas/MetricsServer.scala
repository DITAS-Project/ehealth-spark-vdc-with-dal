/**
 * Copyright 2019 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * This is being developed for the DITAS Project: https://www.ditas-project.eu/
 */
package com.ditas

import com.ditas.ehealth.MetricsService.{GetDataSourceMetricsReply, GetDataSourceMetricsRequest, MetricsServiceGrpc}
import com.ditas.utils.UtilFunctions
import io.grpc.{Server, ServerBuilder}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object MetricsServer {
  private val LOGGER = LoggerFactory.getLogger(classOf[MetricsServer])
  private var port = 50054 //default port
  private var metrics = """{ "CPU": "50.0", "RAM": "1000.0", "Disk": "1000" }"""

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: MetricsServer <configFile>")
      System.exit(1)
    }
    val configFile = UtilFunctions.loadServerConfig(args(0))
    port = configFile.port

    val server = new MetricsServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  class MetricsServiceImpl extends MetricsServiceGrpc.MetricsService {
    override def getDataSourceMetrics(request: GetDataSourceMetricsRequest): Future[GetDataSourceMetricsReply] = {
      val reply = new GetDataSourceMetricsReply(metrics)
      Future.successful(reply)
    }
  }
}

class MetricsServer(executionContext: ExecutionContext) {
  self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    val builder = ServerBuilder.forPort(MetricsServer.port)
    builder.addService(MetricsServiceGrpc.
      bindService(new MetricsServer.MetricsServiceImpl, executionContext))

    server = builder.build().start()

    MetricsServer.LOGGER.info("Server started, listening on " + MetricsServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }


  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }
}
