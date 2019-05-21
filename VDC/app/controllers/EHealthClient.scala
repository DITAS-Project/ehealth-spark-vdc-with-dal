package controllers

import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties
import com.ditas.ehealth.EHealthService.{EHealthQueryReply, EHealthQueryRequest, EHealthQueryServiceGrpc}
import io.grpc._

import scala.concurrent.ExecutionContext

object EHealthClient {

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
      case ex: StatusRuntimeException => println(ex.getStatus.getDescription)
    }
    reply
  }

}