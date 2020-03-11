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

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileFilter, FileInputStream, FileOutputStream}
import java.nio.file.{Path, Paths}

import com.ditas.configuration.ServerConfiguration
import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties
import com.ditas.ehealth.DalPrivacyProperties.DalPrivacyProperties.PrivacyZone
import com.ditas.ehealth.DataMovementService._
import com.ditas.utils.UtilFunctions
import io.grpc.{Server, ServerBuilder, Status}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPFile, FTPReply}

import scala.concurrent.{ExecutionContext, Future}

object DataMovementServer {

  private val LOGGER = LoggerFactory.getLogger(classOf[DataMovementServer])
  private val FTP_URL_PROP = "FTP_URL"
  private val FTP_USERNAME_PROP = "FTP_USERNAME"
  private val FTP_PASSWORD_PROP = "FTP_PASSWORD"


  val parallelism = 4
  lazy val spark: SparkSession = SparkSession.builder.appName(sparkAppName)
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
    .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", ServerConfigFile.sparkHadoopF3S3AConfig.get("spark.hadoop.fs.AbstractFileSystem.s3a.impl"))
    .config("spark.ui.enabled", false).getOrCreate()

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
    val DEFAULT_PUBLIC_PRIVACY_PROPERTIES = new DalPrivacyProperties(PrivacyZone.PUBLIC)
    val FTP_SERVER_FOLDER_PREFIX = "/dm"

    override def startDataMovement(request: StartDataMovementRequest): Future[StartDataMovementReply] = {

      var purpose = request.dalMessageProperties.get.purpose
      val authorization = request.dalMessageProperties.get.authorization
      var query = request.query
      val queryParameters = request.queryParameters
      val sharedVolumePath = request.sharedVolumePath
      val sourcePrivacyZone = request.sourcePrivacyProperties.getOrElse(DEFAULT_PUBLIC_PRIVACY_PROPERTIES).privacyZone
      val destinationPrivacyZone = request.destinationPrivacyProperties.getOrElse(DEFAULT_PUBLIC_PRIVACY_PROPERTIES).privacyZone

      val accessType = "read" //getAccessType(sourcePrivacyZone, destinationPrivacyZone)
      if(purpose.equalsIgnoreCase("data_movement_public_cloud") && query.toLowerCase.startsWith("select * from blood_tests")) {
        //need to do join.
        //assume the query is : SELECT * FROM blood_tests WHERE ..."

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

        val where_filter =  if(query.toLowerCase.contains("where"))  " AND (" + query.slice(query.toLowerCase().indexOf("where") + 5, query.length) + ") "  else ""

        val IDsTbl = " (SELECT DISTINCT blood_tests.patientId FROM blood_tests WHERE blood_tests.stroke==1) "
        val joinedTbl = "blood_tests INNER JOIN patientsProfiles ON patientsProfiles.patientId=blood_tests.patientId"

        query = "(SELECT    0 as stroke, " + all_cols + "  FROM " + joinedTbl + " WHERE   blood_tests.category==\'blood_test\' AND (blood_tests.patientId NOT IN " + IDsTbl + ") "+  where_filter  + ")" +
          " UNION " +
          "(SELECT     1 as stroke, " + all_cols + "  FROM " + joinedTbl + " WHERE   blood_tests.category==\'blood_test\' AND (blood_tests.patientId  IN " + IDsTbl + ") "+  where_filter  + ")"
      }

      var response: Future[StartDataMovementReply] = null
      try {
        val resultDF: DataFrame = DataMovementServer.queryImpl.internalQuery(query, queryParameters, purpose, accessType, authorization, sharedVolumePath)
        response = createResponse(resultDF)
      } catch {
        case e: Exception => DataMovementServer.LOGGER.error("Exception in process engine response " + e, e);
          response = Future.failed(Status.INTERNAL.augmentDescription(e.getMessage).asRuntimeException())
      }
      publishByFTP(sharedVolumePath, FTP_SERVER_FOLDER_PREFIX)

      response
    }

    private def getAccessType(sourcePrivacyZone: PrivacyZone, destinationPrivacyZone: PrivacyZone) = {
      "read_" + sourcePrivacyZone.toString() + "_" + destinationPrivacyZone.toString()
    }

    override def finishDataMovement(request: FinishDataMovementRequest): Future[FinishDataMovementReply] = {
      val purpose = request.dalMessageProperties.get.purpose
      val authorization = request.dalMessageProperties.get.authorization
      val query = request.query
      val queryParameters = request.queryParameters
      val sharedVolumePath = request.sharedVolumePath
      val targetDatasource = request.targetDatasource
      val sourcePrivacyZone = request.sourcePrivacyProperties.getOrElse(DEFAULT_PUBLIC_PRIVACY_PROPERTIES).privacyZone
      val destinationPrivacyZone = request.destinationPrivacyProperties.getOrElse(DEFAULT_PUBLIC_PRIVACY_PROPERTIES).privacyZone

      val accessType = "write" //getAccessType(sourcePrivacyZone, destinationPrivacyZone)

      var response: Future[FinishDataMovementReply] = null
      try {
        if (downloadByFTP(sharedVolumePath, FTP_SERVER_FOLDER_PREFIX) < 1) {
          response = Future.failed(Status.INTERNAL.augmentDescription("No files downloaded").asRuntimeException())
        } else {
          DataMovementServer.queryImpl.persistQueryResult(query, queryParameters, purpose, accessType, authorization, sharedVolumePath)
          response = Future.successful(new FinishDataMovementReply)
        }
      } catch {
        case e: Exception => DataMovementServer.LOGGER.error("Exception in process engine response " + e, e);
          response = Future.failed(Status.INTERNAL.augmentDescription(e.getMessage).asRuntimeException())
      }
      response
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

    def publishByFTP(localPath: String, remotePath: String): Unit = {
      val envProperty = DataMovementServer.FTP_URL_PROP
      val ftpUrl = getEnvProperty(envProperty)
      val ftpUsername = getEnvProperty(DataMovementServer.FTP_USERNAME_PROP)
      val ftpPassword = getEnvProperty(DataMovementServer.FTP_PASSWORD_PROP)

      if ((null == ftpUrl) || ftpUrl.isEmpty || (null == ftpUsername) || ftpUsername.isEmpty ||
        (null == ftpPassword) || ftpPassword.isEmpty) {
        DataMovementServer.LOGGER.warn(s"FTP server is not fully defined in environment variables: ${DataMovementServer.FTP_URL_PROP}, " +
          s"${DataMovementServer.FTP_USERNAME_PROP}, ${DataMovementServer.FTP_PASSWORD_PROP}")
        return
      }
      println("Publish by FTP")
      val ftpClient = new FTPClient()
      ftpClient.connect(ftpUrl)
      ftpClient.login(ftpUsername, ftpPassword)
      try {
        if (!ftpClient.changeWorkingDirectory(remotePath))
          ftpClient.makeDirectory(remotePath)
        val parquetDirName = Paths.get(localPath).getFileName.toString
        if (!ftpClient.changeWorkingDirectory(remotePath + File.separator + parquetDirName))
          ftpClient.makeDirectory(remotePath + File.separator + parquetDirName)
        val localDir = new File(localPath)
        val files = localDir.listFiles().filter(_.isFile)
        files.foreach { localFile =>
          val fullLocalFilename = localPath + File.separator + localFile.getName
          val fullTargetPath = remotePath + File.separator + parquetDirName + File.separator + localFile.getName
          println(s"Starting upload of ${fullLocalFilename} to ${ftpUrl} ${fullTargetPath}")
          val fileInputStream = new BufferedInputStream(new FileInputStream(fullLocalFilename))
          ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
          ftpClient.storeFile(fullTargetPath, fileInputStream)
          fileInputStream.close()
          println(s"Finished upload of ${fullLocalFilename} to ${ftpUrl} ${fullTargetPath}")
        }
      }
      catch {
        case ex: Throwable =>
          DataMovementServer.LOGGER.error(s"Failed to upload data movement result to: ${DataMovementServer.FTP_URL_PROP}", ex)
      }
      finally {
        ftpClient.logout()
        ftpClient.disconnect()
      }
    }

    def downloadByFTP(localPath: String, remotePath: String): Int = {
      val ftpUrl = getEnvProperty(DataMovementServer.FTP_URL_PROP)
      val ftpUsername = getEnvProperty(DataMovementServer.FTP_USERNAME_PROP)
      val ftpPassword = getEnvProperty(DataMovementServer.FTP_PASSWORD_PROP)
      var numFilesDownloaded = 0

      if ((null == ftpUrl) || ftpUrl.isEmpty || (null == ftpUsername) || ftpUsername.isEmpty ||
        (null == ftpPassword) || ftpPassword.isEmpty) {
        DataMovementServer.LOGGER.warn(s"FTP server is not fully defined in environment variables: ${DataMovementServer.FTP_URL_PROP}, " +
          s"${DataMovementServer.FTP_USERNAME_PROP}, ${DataMovementServer.FTP_PASSWORD_PROP}")
        return numFilesDownloaded
      }
      println("Download by FTP")
      val ftpClient = new FTPClient()
      ftpClient.connect(ftpUrl)
      ftpClient.login(ftpUsername, ftpPassword)

      try {
        ftpClient.changeWorkingDirectory(remotePath)
        val localDir = new File(localPath)
        localDir.mkdirs()
        val parquetDirName = Paths.get(localPath).getFileName.toString
        val files = ftpClient.listFiles(remotePath + File.separator + parquetDirName)
        files.foreach { remoteFile =>
          val localFilePath = localPath + File.separator + remoteFile.getName
          val fileOutputStream = new BufferedOutputStream(new FileOutputStream(localFilePath))
          val remoteFullPath = remotePath + File.separator + parquetDirName + File.separator + remoteFile.getName
          println(s"Starting download of ${remoteFullPath} from ${ftpUrl} to ${localFilePath}")
          ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
          ftpClient.retrieveFile(remoteFullPath, fileOutputStream)
          fileOutputStream.close()
          numFilesDownloaded+= 1
          println(s"Finished download of ${localFilePath} from ${ftpUrl} to ${localPath}")
        }
      } catch {
        case ex: Throwable =>
          DataMovementServer.LOGGER.error(s"Failed to download data movement result from: ${DataMovementServer.FTP_URL_PROP}", ex)
      } finally {
        ftpClient.logout()
        ftpClient.disconnect()
      }
      numFilesDownloaded
    }
  }

  private def getEnvProperty(propertyName: String) = {
    var propertyValue = System.getenv(propertyName)
    if (null == propertyValue) {
      propertyValue = System.getProperty(propertyName)
    }
    propertyValue
  }
}










