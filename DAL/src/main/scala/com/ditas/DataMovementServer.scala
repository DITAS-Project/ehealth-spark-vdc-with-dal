package com.ditas

import com.ditas.configuration.ServerConfiguration
import com.ditas.ehealth.DataMovementService._
import com.ditas.utils.UtilFunctions
import io.grpc.{Server, ServerBuilder, Status}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object DataMovementServer {

  private val LOGGER = LoggerFactory.getLogger(classOf[DataMovementServer])
  val parallelism = 4
  lazy val spark: SparkSession = SparkSession.builder.appName(sparkAppName)
//    .master("spark://mayaa-dev.sl.cloud9.ibm.com:7077")
          .master("local")
    .config("spark.sql.shuffle.partitions", parallelism)
    .config("spark.default.parallelism", parallelism)
    .config("spark.executor.cores", parallelism)
    .config("spark.driver.cores", 1)
    .config("spark.hadoop.fs.s3a.endpoint", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.endpoint"))
    .config("spark.hadoop.fs.s3a.access.key", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.access.key"))
    .config("spark.hadoop.fs.s3a.secret.key", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.secret.key"))
    .config("spark.hadoop.fs.s3a.path.style.access", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.path.style.access"))
    .config("spark.hadoop.fs.s3a.impl", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.s3a.impl"))
    .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.AbstractFileSystem.s3a.impl")).getOrCreate()




  lazy val queryImpl = new QueryImpl(spark, ServerConfigFile)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: DataMovementServer <configFile>")
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

    val server = new DataMovementServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
    spark.close()
  }

  var port = 50055 //default port
  var debugMode = false
  var sparkAppName = "DataMovementServer"
  var ServerConfigFile: ServerConfiguration = null

}


class DataMovementServer(executionContext: ExecutionContext) {
  self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    val builder = ServerBuilder.forPort(DataMovementServer.port)
    builder.addService(DataMovementServiceGrpc.
      bindService(new DataMovementServiceImpl, executionContext))

    server = builder.build().start()

    DataMovementServer.LOGGER.info("Server started, listening on " + DataMovementServer.port)
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


  private class DataMovementServiceImpl extends DataMovementServiceGrpc.DataMovementService {
    override def startDataMovement(request: StartDataMovementRequest): Future[StartDataMovementReply] = {

      val purpose = request.dalMessageProperties.get.purpose
      val authorization = request.dalMessageProperties.get.authorization
      val query = request.query
      val queryParameters = request.queryParameters
      val sharedVolumePath = request.sharedVolumePath
      val sourcePrivacyProperties = request.sourcePrivacyProperties
      val destinationPrivacyProperties = request.destinationPrivacyProperties
      // TODO decide on parquet properties based on these source and target properties

      var response: Future[StartDataMovementReply] = null
      try {
        val resultDF: DataFrame = DataMovementServer.queryImpl.internalQuery(query, queryParameters, purpose, authorization, sharedVolumePath)
        response = createResponse(resultDF)
      } catch {
        case e: Exception => DataMovementServer.LOGGER.error("Exception in process engine response " + e, e);
          response = Future.failed(Status.INTERNAL.augmentDescription(e.getMessage).asRuntimeException())
      }
      response
    }


    override def finishDataMovement(request: FinishDataMovementRequest): Future[FinishDataMovementReply] = {
      val sharedVolumePath = request.sharedVolumePath
      val targetDatasource = request.targetDatasource

      val spark = DataMovementServer.spark
      val dataDF = spark.read.parquet(sharedVolumePath);
      dataDF.write.mode(SaveMode.Overwrite).parquet(targetDatasource)
      Future.successful(new FinishDataMovementReply)
    }

    def createResponse(resultDF: DataFrame): Future[StartDataMovementReply] = {
      if (resultDF == DataMovementServer.spark.emptyDataFrame) {
        //TODO: make the error message more informative
        Future.failed(Status.ABORTED.augmentDescription("Error processing enforcement engine result").asRuntimeException())
      } else {
        val nonNullResultDF = resultDF.filter(row => UtilFunctions.anyNotNull(row))

        if (DataMovementServer.debugMode)
          nonNullResultDF.distinct().show(DataMovementServer.ServerConfigFile.showDataFrameLength, false)

        if (nonNullResultDF == DataMovementServer.spark.emptyDataFrame ||
          nonNullResultDF.count() == 0) {
          DataMovementServer.LOGGER.info("No results were found for the given query")
        }
        Future.successful(new StartDataMovementReply)
      }
    }
  }

}










