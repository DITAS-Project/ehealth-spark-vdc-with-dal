package com.ditas


import com.ditas.configuration.ServerConfiguration
import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties
import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties.PrivacyZone
import com.ditas.ehealth.EHealthService.{EHealthQueryReply, EHealthQueryRequest, EHealthQueryServiceGrpc}
import com.ditas.utils.{JwtValidator, UtilFunctions}
import io.grpc._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import play.api.libs.json._
import scalaj.http.{Http, HttpOptions, HttpResponse}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object EhealthServer {
  private val LOGGER = LoggerFactory.getLogger(classOf[EhealthServer])
  lazy val spark: SparkSession = SparkSession.builder.appName(EhealthServer.sparkAppName).master("local")
    .config("spark.hadoop.fs.s3a.endpoint", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.endpoint"))
    .config("spark.hadoop.fs.s3a.access.key", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.access.key"))
    .config("spark.hadoop.fs.s3a.secret.key", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.secret.key"))
    .config("spark.hadoop.fs.s3a.path.style.access", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.path.style.access"))
    .config("spark.hadoop.fs.s3a.impl", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.impl"))
    .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", EhealthServer.ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.AbstractFileSystem.s3a.impl")).getOrCreate()

  val DEFAULT_PUBLIC_PRIVACY_PROPERTIES = new DalPrivacyProperties(PrivacyZone.PUBLIC)


  lazy val queryImpl = new QueryImpl(spark, ServerConfigFile)

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

  private var validRoles: mutable.Buffer[String] = ArrayBuffer("*")


  private def sendRequestToEnforcmentEngine(purpose: String, requesterId: String, authorizationHeader: String, enforcementEngineURL: String,
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

    EhealthServer.LOGGER.info("Enforcement engine returned code " + response.code);
    if (response.code != 200) {
      EhealthServer.LOGGER.info("Enforcement engine returned code " + response.code);
      res_query = ""
    }
    return res_query
  }

  private def createDataAndProfileJoinDataFrame(spark: SparkSession, response: String): Boolean = {

    var bloodTestsCompliantDF: DataFrame = null
//    try {
      if (EhealthServer.debugMode) {
        println("Response before replacement: " + response)
      }
      var eeResponse = response.replace("blood_tests_", "blood_tests.").replace("blood_tests.clauses", "blood_tests_clauses")
      eeResponse = eeResponse.replace("patientsProfiles_", "patientsProfiles.").replace("patientsProfiles.clauses", "patientsProfiles_clauses")
      if (EhealthServer.debugMode) {
        println("Response after replacement: " + eeResponse)
      }
      bloodTestsCompliantDF = EnforcementEngineResponseProcessor.processResponse(spark, EhealthServer.ServerConfigFile,
        eeResponse, EhealthServer.debugMode, EhealthServer.ServerConfigFile.showDataFrameLength)
//    } catch {
//      case e: Exception => EhealthServer.LOGGER.error("Exception in process engine response " + e, e);
//        return false
//    }
    if (bloodTestsCompliantDF == spark.emptyDataFrame)
      return false


    bloodTestsCompliantDF.createOrReplaceTempView("joined")
    if (EhealthServer.debugMode) {
      println("===========" + "JOINED bloodTests and profiles" + "===========")
      bloodTestsCompliantDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)
    }
    return true
    /*
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
    */
  }

  private def getCompliantBloodTestsAndProfiles(spark: SparkSession, queryOnJoinedTable: String,
                                                dataAndProfileJoin: String): DataFrame = {
    if (!createDataAndProfileJoinDataFrame(spark, dataAndProfileJoin)) {
      EhealthServer.LOGGER.info("Error in createDataAndProfileJoinDataFrame")
      return spark.emptyDataFrame
    }

    var patientBloodTestsDF = spark.sql("select * from joined").toDF().filter(row => UtilFunctions.anyNotNull(row))
    //    var patientBloodTestsDF = spark.sql(query).toDF()
    if (EhealthServer.debugMode) {
      println(queryOnJoinedTable)
      patientBloodTestsDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)
      patientBloodTestsDF.printSchema
      patientBloodTestsDF.explain(true)
    }
    patientBloodTestsDF
  }


  class EHealthQueryServiceImpl extends EHealthQueryServiceGrpc.EHealthQueryService {
    private val jwtValidation = new JwtValidator(EhealthServer.ServerConfigFile)

    override def query(request: EHealthQueryRequest): Future[EHealthQueryReply] = {
      return internalQuery(request, null/*responseParquetPath*/)
    }

    def queryToParquetFile(request: EHealthQueryRequest, responseParquetPath: String): Future[EHealthQueryReply] = {
      return internalQuery(request, responseParquetPath)
    }

    def internalQuery(request: EHealthQueryRequest, responseParquetPath: String): Future[EHealthQueryReply] = {

      var queryObject = request.query
      val queryParameters = request.queryParameters
      val purpose = request.dalMessageProperties.get.purpose
      val authorization = request.dalMessageProperties.get.authorization

      var dalPrivacyZone = request.dalPrivacyProperties.getOrElse(DEFAULT_PUBLIC_PRIVACY_PROPERTIES).privacyZone

           def logAndReturnError(errorMessage: String) = {
        EhealthServer.LOGGER.error(errorMessage)
        Future.failed(Status.ABORTED.augmentDescription(errorMessage).asRuntimeException())
      }

      if (purpose.isEmpty) {
        val errorMessage = "Missing purpose"
        logAndReturnError(errorMessage)
      } else if (authorization.isEmpty) {
        val errorMessage = "Missing authorization"
        logAndReturnError(errorMessage)
      } else if (queryObject.isEmpty) {
        val errorMessage = "Missing query"
        logAndReturnError(errorMessage)
      } else {
        val authorizationHeader: String = request.dalMessageProperties.get.authorization
        try {
          jwtValidation.validateJwtToken(authorizationHeader, EhealthServer.ServerConfigFile.jwtServerTimeout, EhealthServer.validRoles)
        } catch {
          case e: Exception => {
            EhealthServer.LOGGER.error("query", e);
            return Future.failed(Status.UNAUTHENTICATED.augmentDescription(e.getMessage).asRuntimeException())
          }
        }

        //ETY:
        if (EhealthServer.ServerConfigFile.debugMode) {
          //TEST:
          dalPrivacyZone = new DalPrivacyProperties(PrivacyZone.PRIVATE).privacyZone
          println("Changed privacy mode for test to " + dalPrivacyZone.toString())
        }

        //ssn is cahed in private cloud. NOTICE: the exact string search, any whitespace can ruin it!
        if(dalPrivacyZone.toString() == "PRIVATE" && queryObject.contains("socialId='")) {
          val beginSSN = queryObject.indexOf("socialId='")+"socialId=".length
          val endSSN = queryObject.indexOf("'", beginSSN+1)+1
          val SSN = queryObject.substring(beginSSN, endSSN)
          queryObject = queryObject.substring(0, beginSSN)+"hash("+SSN + ")" + queryObject.substring(endSSN, queryObject.length)

          if (EhealthServer.ServerConfigFile.debugMode) {
            println("HASH(SSN) rewrite query! " + beginSSN, "   ", endSSN, "   ", SSN)
            println("Query with hash: " + queryObject)
          }
        }


        if(dalPrivacyZone.toString() == "PUBLIC" &&  queryObject.contains("INNER JOIN patientsProfiles ON blood_tests.patientId=patientsProfiles.patientId")){
          val queryOnJoined = queryObject.replace("INNER JOIN patientsProfiles ON blood_tests.patientId=patientsProfiles.patientId", "" )
          println("Query on public cloud joined blood_tests, rewritten query: ", queryOnJoined)
          queryObject=queryOnJoined
        }
        ///////////

        EhealthServer.LOGGER.info(s"query: [${queryObject}]")
        val query_lower = queryObject.toLowerCase
        EhealthServer.LOGGER.info(s"query lower case: [${query_lower}]")
        if(query_lower contains "from documents") {
          EhealthServer.LOGGER.info("Query from documents data source")
          DataFrameUtils.addTableToSpark(spark, ServerConfigFile, "documents",
            ServerConfigFile.showDataFrameLength, debugMode)
          var docDF = spark.sql(queryObject).toDF()
          println(docDF)
          docDF.printSchema
          val values = docDF.toJSON
          return Future.successful(new EHealthQueryReply(values.collectAsList().asScala))
        }
        val dataAndProfileGovernedJoin = sendRequestToEnforcmentEngine(purpose, "", authorizationHeader,
          EhealthServer.ServerConfigFile.policyEnforcementUrl, queryObject)

        if (dataAndProfileGovernedJoin == "") {
          Future.failed(Status.ABORTED.augmentDescription("Error in enforcement engine").asRuntimeException())
        }
        else {
          if (EhealthServer.ServerConfigFile.debugMode) {
            println("In Query: " + queryObject)
            println("Query with Enforcement: " + dataAndProfileGovernedJoin)
          }
          val queryOnJoinedTable = queryObject.replaceAll("blood_tests", "joined");
          println(s"Query [${queryObject}] becomes [${queryOnJoinedTable}]")

          val resultDF = getCompliantBloodTestsAndProfiles(EhealthServer.spark, queryOnJoinedTable, dataAndProfileGovernedJoin)

          if (null != responseParquetPath) {
            resultDF.write.parquet(responseParquetPath)
          }
          val response = createResponse(resultDF)
          response
        }
      }
    }

    def createResponse(resultDF: DataFrame): Future[EHealthQueryReply] = {
      if (resultDF == EhealthServer.spark.emptyDataFrame) {
        //TODO: make the error message more informative
        Future.failed(Status.ABORTED.augmentDescription("Error processing enforcement engine result").asRuntimeException())
      } else {
        val nonNullResultDF = resultDF.filter(row => UtilFunctions.anyNotNull(row))

        if (EhealthServer.debugMode)
          nonNullResultDF.distinct().show(EhealthServer.ServerConfigFile.showDataFrameLength, false)

        if (nonNullResultDF == EhealthServer.spark.emptyDataFrame ||
          nonNullResultDF.count() == 0) {
          LOGGER.info("No results were found for the given query")
          Future.successful(new EHealthQueryReply())
        } else {
          val values = nonNullResultDF.toJSON
          Future.successful(new EHealthQueryReply(values.collectAsList().asScala))
        }
      }
    }

  }

}



class EhealthServer(executionContext: ExecutionContext) {
  self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    val builder = ServerBuilder.forPort(EhealthServer.port)
    builder.addService(EHealthQueryServiceGrpc.
      bindService(new EhealthServer.EHealthQueryServiceImpl, executionContext))

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

}




