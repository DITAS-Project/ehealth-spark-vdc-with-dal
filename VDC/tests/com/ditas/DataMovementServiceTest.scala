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
  val serverUrlFRA = "178.22.71.88"
  val serverUrlSJC = "104.36.16.245"
  val serverUrlLocal = "localhost"
  val serverUrl = serverUrlSJC


  val bloodtests_col =
    """
      |blood_tests.category, blood_tests.prothrombinTime_unit, blood_tests.cholesterol_hdl_value, blood_tests.totalWhiteCellCount_unit, blood_tests.fibrinogen_normalRange, blood_tests.antithrombin_value, blood_tests.fibrinogen_unit, blood_tests.haemoglobin_value, blood_tests.antithrombin_unit,
      |                  blood_tests.cholesterol_hdl_unit, blood_tests.cholesterol_hdl_normalRange,  blood_tests.cholesterol_ldl_normalRange, blood_tests.bmi, blood_tests.antithrombin_normalRange,blood_tests.totalWhiteCellCount_normalRange, blood_tests.fibrinogen_value,
      |                  blood_tests.cholesterol_ldl_value, blood_tests.plateletCount_value, blood_tests.cholesterol_total_normalRange,blood_tests.cholesterol_tryglicerides_normalRange, blood_tests.totalWhiteCellCount_value, blood_tests.date, blood_tests.cholesterol_ldl_unit,
      |                  blood_tests.haemoglobin_unit, blood_tests.prothrombinTime_value, blood_tests.cholesterol_tryglicerides_unit, blood_tests.plateletCount_unit, blood_tests.cholesterol_total_value,blood_tests.haemoglobin_normalRange, blood_tests.prothrombinTime_normalRange,
      |                  blood_tests.cholesterol_tryglicerides_value,blood_tests.cholesterol_total_unit
      |
      |""".stripMargin

  val all_cols = "  0 AS patientId, patientsProfiles.gender, year(patientsProfiles.birthDate) AS birthDate,  " + bloodtests_col
  val where_filter = "" // AND (blood_tests.patientId == 1046565446 OR  blood_tests.patientId == 480806951)"
  val IDsTbl = " (SELECT DISTINCT blood_tests.patientId FROM blood_tests WHERE blood_tests.stroke==1) "
  val joinedTbl = "blood_tests INNER JOIN patientsProfiles ON patientsProfiles.patientId=blood_tests.patientId"

  val dataMovementToPublicCloudQuery = "(SELECT    0 as stroke, " + all_cols + "  FROM " + joinedTbl + " WHERE   blood_tests.category==\'blood_test\' AND (blood_tests.patientId NOT IN " + IDsTbl + ") "+  where_filter  + ")" +
    " UNION " +
    "(SELECT     1 as stroke, " + all_cols + "  FROM " + joinedTbl + " WHERE   blood_tests.category==\'blood_test\' AND (blood_tests.patientId  IN " + IDsTbl + ") "+  where_filter  + ")"

  @Test
  def testStartDataMovement = {
    val query = "SELECT date from blood_tests LIMIT 5"
//    val query = "SELECT patientId from blood_tests"
    val sharedVolumePath = "./data_to_move.parquet"

//    startDataMovement(query, sharedVolumePath, "move_to_private")
    startDataMovement(query, sharedVolumePath, "Research")
  }

//  @Test
//  def testStartDataMovementToPrivateCloud = {
//    val query = "SELECT * from blood_tests"
//    val sharedVolumePath = "./data_to_move.parquet"
//
//    //verify patientid is hashed
//    startDataMovement(query, sharedVolumePath, "data_movement_private_cloud")
//  }

  @Test
  def testStartDataMovementToPublicCloud = {
//    val query = "SELECT * from blood_tests WHERE blood_tests.patientId == 1046565446 OR  blood_tests.patientId == 480806951"
    val query = "SELECT * from blood_tests"
    val sharedVolumePath = "./data_to_move.parquet"

    //verify joined table
    startDataMovement(query, sharedVolumePath, "data_movement_public_cloud")
  }

  @Test
  def testStartEtyTest = {
    val query = "SELECT cholesterol_total_value, blood_tests.patientId, patientsProfiles.birthDate  from blood_tests INNER JOIN patientsProfiles ON blood_tests.patientId=patientsProfiles.patientId WHERE patientsProfiles.birthDate >= \"2000\" AND patientsProfiles.birthDate < \"2012\" LIMIT 20"
    //    val query = "SELECT * from blood_tests"
    val sharedVolumePath = "./data_to_move.parquet"

    //verify joined table
    startDataMovement(query, sharedVolumePath, "Research")
  }


  @Test
  def testStartDataMovementToPublicCloudTEST = {
    val sharedVolumePath = "./data_to_move.parquet.encrypted"
    val query = "SELECT * from blood_tests"
//    val query = dataMovementToPublicCloudQuery
    println(query)
    startDataMovement(query, sharedVolumePath, "data_movement_public_cloud")
  }

  @Test
  def testStartDataMovementJDBC = {
//    val query = "SELECT patientId from patientsProfiles"
    val sharedVolumePath = "./data_to_move_pp.parquet"
    val query = dataMovementToPublicCloudQuery
    startDataMovement(query, sharedVolumePath)
  }

  private def startDataMovement(query: String, sharedVolumePath: String, purpose: String = "data_movement_public_cloud") = {
    val dalMesssageProperties = new DalMessageProperties(purpose, "requester", "Bearer " + token)
    val sourcePrivacyProperties = None
    val destinationPrivacyProperties = None
    DataMovementClient.startDataMovement(query, sharedVolumePath, Option(dalMesssageProperties), sourcePrivacyProperties,
      destinationPrivacyProperties, serverPort = 50055, serverUrl)
  }

  @Test
  def testFinishDataMovement: Unit = {
    val query = "SELECT * from blood_tests"
    val sharedVolumePath = "./data_to_move.parquet.encrypted"
//    val sharedVolumePath = "./data_to_move__joined.parquet"

    finishDataMovement(query, sharedVolumePath)
  }


  @Test
  def testFinishDataMovementJDBC: Unit = {
    val query = "SELECT patientId from patientsProfiles"
    val sharedVolumePath = "./data_to_move_pp.parquet"

    finishDataMovement(query, sharedVolumePath)
  }

  private def finishDataMovement(query: String, sharedVolumePath: String) = {
    val dalMesssageProperties = new DalMessageProperties("data_movement_public_cloud", "requester", "Bearer " + token)
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
      }
    }
  }
}
