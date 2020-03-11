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
package controllers

import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties
import com.ditas.ehealth.EHealthService.{EHealthQueryReply, EHealthQueryRequest, EHealthQueryServiceGrpc}
import io.grpc._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object EHealthClient {
  private val LOGGER = LoggerFactory.getLogger(EHealthClient.getClass)

  def query(query: String, queryParameters: Seq[String], authorization: String,
            purpose: String, serverPort: Int, serverUrl: String): EHealthQueryReply = {
    implicit val ec = ExecutionContext.global
    var reply: EHealthQueryReply = null

    val channel = ManagedChannelBuilder.forTarget(serverUrl + ":" +
      serverPort).usePlaintext(true).
      build

    val dalMessageProperties: com.ditas.ehealth.DalMessageProperties.DalMessageProperties =
      new com.ditas.ehealth.DalMessageProperties.DalMessageProperties (purpose, requesterId = "", authorization)
    val dalPrivacyProperties: DalPrivacyProperties =
      new DalPrivacyProperties()
    val request = EHealthQueryRequest(Option(dalMessageProperties), Option(dalPrivacyProperties), query, queryParameters)
    val blockingStub = EHealthQueryServiceGrpc.blockingStub(channel)

    try {
      reply = blockingStub.query(request)
      println(s"Num values: ${reply.values.length}")
    } catch {
      case ex: StatusRuntimeException =>
        val errorMessage = s"${ex.getStatus.getCode}. Query threw exception: ${ex.getStatus.getDescription} ."
        println(errorMessage)
        LOGGER.error(errorMessage, ex)
        throw new RuntimeException(errorMessage)
    }
    reply
  }

}
