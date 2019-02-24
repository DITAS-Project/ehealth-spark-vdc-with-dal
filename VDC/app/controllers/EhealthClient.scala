package controllers

import io.grpc._
import com.ditas.ehealth.AvgRequest.{BloodTestComponentAverageGrpc, BloodTestComponentAverageReply, BloodTestComponentAverageRequest}
import com.ditas.ehealth.AllValuesRequest.{AllValuesForBloodTestComponentGrpc, AllValuesForBloodTestComponentReply, AllValuesForBloodTestComponentRequest}
import io.grpc.stub.MetadataUtils
import io.grpc.Metadata


import scala.concurrent.ExecutionContext

object EhealthClient {

  def getBloodTestComponentAverage(startAgeRange: Integer, endAgeRange: Integer, testType: String,
                                   purpose: String, serverPort: Int, serverUrl: String): BloodTestComponentAverageReply = {
    implicit val ec = ExecutionContext.global
    var reply: BloodTestComponentAverageReply = null

    val channel = ManagedChannelBuilder.forTarget(serverUrl + ":" +
      serverPort).usePlaintext(true).
      build

    val dalMessageProperties: com.ditas.ehealth.DalMessageProperties.DalMessageProperties =
      new com.ditas.ehealth.DalMessageProperties.DalMessageProperties (purpose)
    val request = BloodTestComponentAverageRequest(testType, startAgeRange, endAgeRange, Option(dalMessageProperties))
    val blockingStub = BloodTestComponentAverageGrpc.blockingStub(channel)

    try {
      reply = blockingStub.askForBloodTestComponentAverage(request)
      println(reply.value)
    } catch {
      case ex: StatusRuntimeException => println(ex.getStatus.getDescription)
    }
    reply
  }

  def getAllValuesForBloodTestComponent(socialId: String, testType: String, purpose: String, requesterId: String,
                                        serverPort: Int, serverUrl: String): AllValuesForBloodTestComponentReply = {
    implicit val ec = ExecutionContext.global
    var reply: AllValuesForBloodTestComponentReply = null
    val channel = ManagedChannelBuilder.forTarget(serverUrl + ":" +
      serverPort).usePlaintext(true).
      build

    val blockingStub = AllValuesForBloodTestComponentGrpc.blockingStub(channel)

    var dalMessageProperties: com.ditas.ehealth.DalMessageProperties.DalMessageProperties =
      new com.ditas.ehealth.DalMessageProperties.DalMessageProperties (purpose, requesterId)

    val request = AllValuesForBloodTestComponentRequest(socialId, testType, Option(dalMessageProperties))
    try {
      reply = blockingStub.askForAllValuesForBloodTestComponent(request)
      println(reply.toProtoString)
    } catch {
      case ex: StatusRuntimeException => println(ex.getStatus.getDescription)
    }
    reply
  }
}
