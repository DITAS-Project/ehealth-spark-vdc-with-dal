package com.ditas

import com.ditas.configuration.ServerConfiguration
import com.ditas.utils.{JwtValidator, UtilFunctions}
import io.grpc._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import play.api.libs.json._
import scalaj.http.{Http, HttpOptions, HttpResponse}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object QueryImpl {
  private val LOGGER = LoggerFactory.getLogger(classOf[QueryImpl])
  private var debugMode = false

  private var validRoles: mutable.Buffer[String] = ArrayBuffer("*")
}

class QueryImpl(spark: SparkSession, configFile: ServerConfiguration) {
  private var serverConfigFile: ServerConfiguration = configFile
  private val jwtValidation = new JwtValidator(serverConfigFile)

  def sendRequestToEnforcementEngine(purpose: String, requesterId: String, authorizationHeader: String, enforcementEngineURL: String,
                                     query: String): String = {
    val data = Json.obj(
      "query" -> query,
      "purpose" -> purpose,
      "access" -> "read",
      "requester" -> "",
      "blueprintId" -> "",
      "requesterId" -> requesterId
    )

    val inf = 1000000

    val response: HttpResponse[String] = Http(enforcementEngineURL).postData(data.toString())
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .header("accept", "application/json")
      .header("authorization", authorizationHeader)
      .option(HttpOptions.readTimeout(inf)).asString

    var res_query: String = null
    res_query = response.body

    QueryImpl.LOGGER.info("Enforcement engine returned code " + response.code);
    if (response.code != 200) {
      QueryImpl.LOGGER.info("Enforcement engine returned code " + response.code);
      res_query = ""
    }
    return res_query
  }

  private def createDataAndProfileJoinDataFrame(spark: SparkSession, response: String): Boolean = {

    var bloodTestsCompliantDF: DataFrame = null
    try {
      var eeResponse = response.replace("blood_tests_patientId", "blood_tests.patientId")
      eeResponse = eeResponse.replace("patientsProfiles_patientId", "patientsProfiles.patientId")

      bloodTestsCompliantDF = EnforcementEngineResponseProcessor.processResponse(spark, serverConfigFile,
        eeResponse, QueryImpl.debugMode, serverConfigFile.showDataFrameLength)
    } catch {
      case e: Exception => QueryImpl.LOGGER.error("Exception in process engine response " + e, e);
        return false
    }
    if (bloodTestsCompliantDF == spark.emptyDataFrame)
      return false


    bloodTestsCompliantDF.createOrReplaceTempView("joined")
    if (QueryImpl.debugMode) {
      println("===========" + "JOINED bloodTests and profiles" + "===========")
      bloodTestsCompliantDF.distinct().show(serverConfigFile.showDataFrameLength, false)
    }
    return true

  }

  private def getCompliantBloodTestsAndProfiles(spark: SparkSession, queryOnJoinedTable: String,
                                                dataAndProfileJoin: String): DataFrame = {
    if (!createDataAndProfileJoinDataFrame(spark, dataAndProfileJoin)) {
      QueryImpl.LOGGER.info("Error in createDataAndProfileJoinDataFrame")
      return spark.emptyDataFrame
    }

    var patientBloodTestsDF = spark.sql(queryOnJoinedTable).toDF().filter(row => UtilFunctions.anyNotNull(row))
    //    var patientBloodTestsDF = spark.sql(query).toDF()
    if (QueryImpl.debugMode) {
      println(queryOnJoinedTable)
      patientBloodTestsDF.distinct().show(serverConfigFile.showDataFrameLength, false)
      patientBloodTestsDF.printSchema
      patientBloodTestsDF.explain(true)
    }
    patientBloodTestsDF
  }


  def internalQuery(queryObject: String, queryParameters: Seq[String], purpose: String, authorization: String, responseParquetPath: String): DataFrame = {

    if (purpose.isEmpty) {
      val errorMessage = "Missing purpose"
      throw new RequestException(errorMessage)
    } else if (authorization.isEmpty) {
      val errorMessage = "Missing authorization"
      throw new RequestException(errorMessage)
    } else if (queryObject.isEmpty) {
      val errorMessage = "Missing query"
      throw new RequestException(errorMessage)
    }
    try {
      jwtValidation.validateJwtToken(authorization, serverConfigFile.jwtServerTimeout, QueryImpl.validRoles)
    } catch {
      case e: Exception => {
        QueryImpl.LOGGER.error("query", e);
        throw e
      }
    }

    val dataAndProfileGovernedJoin = sendRequestToEnforcementEngine(purpose, "", authorization,
      serverConfigFile.policyEnforcementUrl, queryObject)

    if (dataAndProfileGovernedJoin == "") {
      val errorMessage = "Error in enforcement engine"
      throw new RequestException(errorMessage)
    }
    else {
      if (serverConfigFile.debugMode) {
        println("In Query: " + queryObject)
        println("Query with Enforcement: " + dataAndProfileGovernedJoin)
      }
      val queryOnJoinedTable = queryObject.replaceAll("blood_tests", "joined");
      println(s"Query [${queryObject}] becomes [${queryOnJoinedTable}]")

      val resultDF = getCompliantBloodTestsAndProfiles(spark, queryOnJoinedTable, dataAndProfileGovernedJoin)

      if (null != responseParquetPath) {
        resultDF.write.mode(SaveMode.Overwrite).parquet(responseParquetPath)
      }
      resultDF
    }
  }

  def persistQueryResult(queryObject: String, queryParameters: Seq[String], purpose: String, authorization: String,
                         sharedVolumePath: String) = {
    if (purpose.isEmpty) {
      val errorMessage = "Missing purpose"
      throw new RequestException(errorMessage)
    } else if (authorization.isEmpty) {
      val errorMessage = "Missing authorization"
      throw new RequestException(errorMessage)
    } else if (queryObject.isEmpty) {
      val errorMessage = "Missing query"
      throw new RequestException(errorMessage)
    }
    try {
      jwtValidation.validateJwtToken(authorization, serverConfigFile.jwtServerTimeout, QueryImpl.validRoles)
    } catch {
      case e: Exception => {
        QueryImpl.LOGGER.error("query", e);
        throw e
      }
    }
    val dataAndProfileGovernedJoin = sendRequestToEnforcementEngine(purpose, "", authorization,
      serverConfigFile.policyEnforcementUrl, queryObject)

    if (dataAndProfileGovernedJoin == "") {
      val errorMessage = "Error in enforcement engine"
      throw new RequestException(errorMessage)
    }
    else {
      if (serverConfigFile.debugMode) {
        println("In Query: " + queryObject)
        println("Query with Enforcement: " + dataAndProfileGovernedJoin)
      }
      val spark = DataMovementServer.spark
      val dataDF = spark.read.parquet(sharedVolumePath);
      persistCompliantQueryBloodTestsAndProfiles(dataDF, spark, dataAndProfileGovernedJoin)
    }
  }

  private def persistCompliantQueryBloodTestsAndProfiles(dataDF: DataFrame, spark: SparkSession, dataAndProfileJoin: String) = {
    var eeResponse = dataAndProfileJoin.replace("blood_tests_patientId", "blood_tests.patientId")
    eeResponse = eeResponse.replace("patientsProfiles_patientId", "patientsProfiles.patientId")

    EnforcementEngineResponseProcessor.persistDataBasedOnEEResponse(dataDF, spark, serverConfigFile,
      eeResponse, QueryImpl.debugMode, serverConfigFile.showDataFrameLength)
  }
}
