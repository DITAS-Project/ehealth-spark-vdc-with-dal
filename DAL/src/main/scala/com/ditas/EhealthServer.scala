package com.ditas


import io.grpc._
import java.util.logging.Logger

import com.ditas.configuration.ServerConfiguration
import com.ditas.ehealth.EHealthService.{EHealthQueryReply, EHealthQueryRequest, EHealthQueryServiceGrpc}
import com.ditas.utils.UtilFunctions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import io.grpc.ServerCall.Listener
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent._
import scala.concurrent.duration.Duration
import play.api.libs.json._
import io.grpc.stub.MetadataUtils
import io.grpc.Metadata
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, _}
import scala.collection.JavaConverters._

object EhealthServer {
  private val LOGGER = Logger.getLogger(classOf[EhealthServer].getName)
  lazy val spark: SparkSession = SparkSession.builder.appName(EhealthServer.sparkAppName).master("local")
    .config("spark.hadoop.fs.s3a.endpoint", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.endpoint"))
    .config("spark.hadoop.fs.s3a.access.key", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.access.key"))
    .config("spark.hadoop.fs.s3a.secret.key", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.secret.key"))
    .config("spark.hadoop.fs.s3a.path.style.access", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.path.style.access"))
    .config("spark.hadoop.fs.s3a.impl", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.impl"))
    .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.AbstractFileSystem.s3a.impl")).getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: EhealthServer <configFile>")
      System.exit(1)
    }
    val configFile = UtilFunctions.loadServerConfig(args(0))
    debugMode = configFile.debugMode
    if (configFile.sparkAppName != null)
      sparkAppName = configFile.sparkAppName

    ServerConfigFile = configFile
    port = configFile.port

    if (configFile.policyEnforcementUrl == "") {
      System.err.println("Missing enforcement url")
      System.exit(1)
    }

    val server = new EhealthServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
    spark.close()
  }

  private var port = 50052 //default port
  private var debugMode = false
  private var sparkAppName = "EhealthServer"
  private var ServerConfigFile: ServerConfiguration = null
}



class EhealthServer(executionContext: ExecutionContext) {
  self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    val builder = ServerBuilder.forPort(EhealthServer.port)
    builder.addService(EHealthQueryServiceGrpc.
      bindService(new EHealthQueryServiceImpl, executionContext))

    server = builder.build().start()

    EhealthServer.LOGGER.info("Server started, listening on " + EhealthServer.port)
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

  private def sendRequestToEnforcmentEngine(purpose: String, requesterId: String, enforcementEngineURL: String,
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
      .header("accept", "application/json").option(HttpOptions.readTimeout(inf)).asString

    var res_query: String = null
    res_query = response.body

    EhealthServer.LOGGER.info("Enforcement engine returned code " + response.code);
    if (response.code != 200) {
      EhealthServer.LOGGER.info("Enforcement engine returned code " + response.code);
      res_query = ""
    }
    return res_query
  }

  private def createDataAndProfileJoinDataFrame(spark: SparkSession, response: String): Boolean = {

    var bloodTestsCompliantDF: DataFrame = null
    try {
      bloodTestsCompliantDF = EnforcementEngineResponseProcessor.processResponse(spark, EhealthServer.ServerConfigFile,
        response, EhealthServer.debugMode, EhealthServer.ServerConfigFile.showDataFrameLength)
    } catch {
      case e: Exception => EhealthServer.LOGGER.info("Exception in process engine response " + e);
        return false
    }
    if (bloodTestsCompliantDF == spark.emptyDataFrame)
      return false
    val profilesDF = DataFrameUtils.loadTableDFFromConfig(null, spark, EhealthServer.ServerConfigFile,
      "patientsProfiles")
    if (EhealthServer.debugMode) {
      profilesDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)
    }
    //This is inner join
    var joinedDF = bloodTestsCompliantDF.join(profilesDF, Constants.SUBJECT_ID_COL_NAME)
    joinedDF.createOrReplaceTempView("joined")
    if (EhealthServer.debugMode) {
      println("===========" + "JOINED bloodTests and profiles" + "===========")
      joinedDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)
    }
    true
  }

  private def getCompliantBloodTestsAndProfiles(spark: SparkSession, query: String,
                                                queryOnJoinTables: String): DataFrame = {
    if (!createDataAndProfileJoinDataFrame(spark, query)) {
      EhealthServer.LOGGER.info("Error in createDataAndProfileJoinDataFrame")
      return spark.emptyDataFrame
    }

    //var patientBloodTestsDF = spark.sql(queryOnJoinTables).toDF().filter(row => UtilFunctions.anyNotNull(row))
    var patientBloodTestsDF = spark.sql(queryOnJoinTables).toDF()
    if (EhealthServer.debugMode) {
      println(queryOnJoinTables)
      patientBloodTestsDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)
      patientBloodTestsDF.printSchema
      patientBloodTestsDF.explain(true)
    }
    patientBloodTestsDF
  }


  private class EHealthQueryServiceImpl extends EHealthQueryServiceGrpc.EHealthQueryService {
    override def query(request: EHealthQueryRequest): Future[EHealthQueryReply] = {

      val queryObject = request.query
      val queryParameters = request.queryParameters
      val purpose = request.dalMessageProperties.get.purpose
      val authorization = request.dalMessageProperties.get.authorization

      import EhealthServer.spark.implicits._

      if (purpose.isEmpty) {
        Future.failed(Status.ABORTED.augmentDescription("Missing purpose").asRuntimeException())
      } else if (authorization.isEmpty) {
        Future.failed(Status.ABORTED.augmentDescription("Missing authorization").asRuntimeException())
      } else if (queryObject.isEmpty) {
        Future.failed(Status.ABORTED.augmentDescription("Missing query").asRuntimeException())
      } else {
        val queryOnJoinTables = sendRequestToEnforcmentEngine(purpose, "",
          EhealthServer.ServerConfigFile.policyEnforcementUrl, queryObject)

        if (queryOnJoinTables == "") {
          Future.failed(Status.ABORTED.augmentDescription("Error in enforcement engine").asRuntimeException())
        }
        else {
          if (EhealthServer.ServerConfigFile.debugMode) {
            println("In Query: " + queryObject)
            println("Query with Enforcement: " + queryOnJoinTables)
          }

//          val queryOnJoinTables = "SELECT " + avgTestType + " FROM joined where birthDate > \"" + minBirthDate + "\" AND birthDate < \"" + maxBirthDate + "\""

          var resultDF = getCompliantBloodTestsAndProfiles(EhealthServer.spark, queryObject, queryOnJoinTables)
          if (resultDF == EhealthServer.spark.emptyDataFrame) {
            //TODO: make the error message more informative
            Future.failed(Status.ABORTED.augmentDescription("Error processing enforcement engine result").asRuntimeException())
          } else {
            //Adjust output to blueprint
//            resultDF = resultDF.withColumnRenamed(avgTestType, "value")

            resultDF = resultDF.filter(row => UtilFunctions.anyNotNull(row))

            if (EhealthServer.debugMode)
              resultDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)

            if (resultDF == EhealthServer.spark.emptyDataFrame ||
              resultDF.count() == 0) {
              Future.failed(Status.ABORTED.augmentDescription("No results were found for the given query").asRuntimeException())
            } else {

              val values = resultDF.toJSON
//              val value = resultDF.map { row => row.getDouble(0) }.first()
//              val values = resultDF.toJSON.map { row =>   }.first()

              Future.successful(new EHealthQueryReply(values.collectAsList().asScala))
            }
          }
        }
      }
    }
  }


}




