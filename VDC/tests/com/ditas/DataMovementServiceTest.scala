package com.ditas

import com.ditas.DataMovementServiceTest.DataMovementClient
import com.ditas.ehealth.DalMessageProperties.DalMessageProperties
import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties
import com.ditas.ehealth.DataMovementService.{DataMovementServiceGrpc, FinishDataMovementRequest, StartDataMovementRequest}
import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.concurrent.ExecutionContext
import scala.io.Source

@RunWith(value = classOf[JUnit4])
class DataMovementServiceTest {
  val token = Source.fromFile("/home/mayaa/Development/GitHub/DITAS/ehealth-spark-vdc-with-dal/DITASconfigFiles/config_files_for_demo/kc_access_token.txt").getLines().mkString
  val serverUrl = "localhost"  // = "178.22.71.88"

  @Test
  def testStartDataMovement = {
    val query = "SELECT patientId from blood_tests"
    val sharedVolumePath = "./data_to_move.parquet"

    startDataMovement(query, sharedVolumePath)
  }

  @Test
  def testStartDataMovementJDBC = {
    val query = "SELECT patientId from patientsProfiles"
    val sharedVolumePath = "./data_to_move_pp.parquet"

    startDataMovement(query, sharedVolumePath)
  }

  private def startDataMovement(query: String, sharedVolumePath: String) = {
    val dalMesssageProperties = new DalMessageProperties("read", "requester", "Bearer " + token)
    val sourcePrivacyProperties = None
    val destinationPrivacyProperties = None
    DataMovementClient.startDataMovement(query, sharedVolumePath, Option(dalMesssageProperties), sourcePrivacyProperties,
      destinationPrivacyProperties, serverPort = 50055, serverUrl)
  }

  @Test
  def testFinishDataMovement: Unit = {
    val query = "SELECT patientId from blood_tests"
    val sharedVolumePath = "./data_to_move.parquet"

    finishDataMovement(query, sharedVolumePath)
  }


  @Test
  def testFinishDataMovementJDBC: Unit = {
    val query = "SELECT patientId from patientsProfiles"
    val sharedVolumePath = "./data_to_move_pp.parquet"

    finishDataMovement(query, sharedVolumePath)
  }

  private def finishDataMovement(query: String, sharedVolumePath: String) = {
    val dalMesssageProperties = new DalMessageProperties("read", "requester", "Bearer " + token)
    val sourcePrivacyProperties = None
    val destinationPrivacyProperties = None
    DataMovementClient.finishDataMovement(query, sharedVolumePath, Option(dalMesssageProperties), sourcePrivacyProperties,
      destinationPrivacyProperties, serverPort = 50055, serverUrl = serverUrl, targetDataSource = "newBloodTests")
  }
}


object DataMovementServiceTest {


  object DataMovementClient {
    def startDataMovement(query: String, sharedVolumePath: String, dalMessageProperties: Option[DalMessageProperties],
                          sourcePrivacyProperties: Option[DalPrivacyProperties], destinationPrivacyProperties: Option[DalPrivacyProperties],
                          serverPort: Int, serverUrl: String) = {
      implicit val ec = ExecutionContext.global
      val channel = ManagedChannelBuilder.forTarget(serverUrl + ":" +
        serverPort).usePlaintext(true).
        build

      val blockingStub = DataMovementServiceGrpc.blockingStub(channel)
      val request = new StartDataMovementRequest(dalMessageProperties, sourcePrivacyProperties,
        destinationPrivacyProperties, query, Seq("") , sharedVolumePath)
      try {
        blockingStub.startDataMovement(request)
        println(s"Started data movement to: ${sharedVolumePath}")
      } catch {
        case ex: StatusRuntimeException =>
          val errorMessage = s"${ex.getStatus.getCode}. Query threw exception: ${ex.getStatus.getDescription} ."
          println(errorMessage)
          throw new RuntimeException(errorMessage)
      }
    }

    def finishDataMovement(query: String, sharedVolumePath: String, dalMessageProperties: Option[DalMessageProperties],
                           sourcePrivacyProperties: Option[DalPrivacyProperties], destinationPrivacyProperties: Option[DalPrivacyProperties],
                           targetDataSource: String,
                           serverPort: Int, serverUrl: String) = {
      implicit val ec = ExecutionContext.global
      val channel = ManagedChannelBuilder.forTarget(serverUrl + ":" +
        serverPort).usePlaintext(true).
        build

      val blockingStub = DataMovementServiceGrpc.blockingStub(channel)
      val request = new FinishDataMovementRequest(dalMessageProperties, sourcePrivacyProperties,
        destinationPrivacyProperties, query, Seq("") , sharedVolumePath, targetDataSource)
      try {
        blockingStub.finishDataMovement(request)
        println(s"Finished data movement from ${sharedVolumePath} to: ${targetDataSource}")
      } catch {
        case ex: StatusRuntimeException =>
          val errorMessage = s"${ex.getStatus.getCode}. Query threw exception: ${ex.getStatus.getDescription} ."
          println(errorMessage)
          throw new RuntimeException(errorMessage)
      }
    }
  }
}
