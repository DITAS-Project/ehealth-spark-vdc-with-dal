package com.ditas


import com.ditas.configuration.ServerConfiguration
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.Stack

object EnforcementEngineResponseProcessor {
  private val LOGGER = LoggerFactory.getLogger("ProcessEnforcementEngineResponse")
  var response : String = ""
  var query: String = ""
  var tableNames: Stack[String] = new Stack[String]()
  var queryOnTables: String = ""
  var debugMode: Boolean = false

  def processResponse (spark: SparkSession, config: ServerConfiguration, response: String, debugMode: Boolean,
                       showDataFrameLength: Int): DataFrame = {
    this.debugMode = debugMode
    val json: JsValue = Json.parse(response)

    val newQuery = (json \ "rewrittenQuery").validate[String]
    query = newQuery.get
    if (debugMode) {
      println("the re-written query: " + newQuery.get)
    }

    setEncryptionPropertiesForSpark(spark, json)
    val tables = (json \ "tables").as[List[JsValue]]
    for (table <- tables) {
      val tableName = (table \ "name").as[String]
      DataFrameUtils.addTableToSpark(spark, config, tableName, showDataFrameLength, debugMode)
    }

    val bloodTestsDF: DataFrame = spark.sql(query).toDF().filter(row => DataFrameUtils.anyNotNull(row))
    if (debugMode) {
      println (query)
      bloodTestsDF.distinct().show(showDataFrameLength, false)
      bloodTestsDF.printSchema
      bloodTestsDF.explain(true)
    }
    bloodTestsDF
  }


  def setEncryptionPropertiesForSpark(spark: SparkSession, json: JsValue) = {
    // Set encryption properties for writing the query result
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val encryptionProperties = (json \ "encryptionProperties").as[List[JsValue]]
    for (encryptionProperty <- encryptionProperties) {
      val key = (encryptionProperty \ "key").as[String]
      val value = (encryptionProperty \ "value").as[String]
      hadoopConfig.set(key, value)
    }
  }

  def persistDataBasedOnEEResponse(tableDF: DataFrame, spark: SparkSession, config: ServerConfiguration, response: String,
                                   debugMode: Boolean, showDataFrameLength: Int) = {
    this.debugMode = debugMode
    val json: JsValue = Json.parse(response)

    setEncryptionPropertiesForSpark(spark, json)
    tableDF.write.mode("append").parquet(config.sparkHadoopF3S3AConfig.get("s3.filename"))

//    val tables = (json \ "tables").as[List[JsValue]]
//    for (table <- tables) {
//      val tableName = (table \ "name").as[String]
//      if (!tableName.endsWith("_clauses") && !tableName.endsWith("_rules") && !tableName.equals("consents")) {
//        DataFrameUtils.writeToSparkTable(tableDF, spark, config, tableName, showDataFrameLength, debugMode)
//      }
//    }

  }
}
