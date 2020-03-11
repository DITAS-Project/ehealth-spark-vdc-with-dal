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
    tableDF.na.fill(0.0, Seq("fibrinogen_value")).write.mode("append").parquet(config.sparkHadoopF3S3AConfig.get("s3.filename"))

//    val tables = (json \ "tables").as[List[JsValue]]
//    for (table <- tables) {
//      val tableName = (table \ "name").as[String]
//      if (!tableName.endsWith("_clauses") && !tableName.endsWith("_rules") && !tableName.equals("consents")) {
//        DataFrameUtils.writeToSparkTable(tableDF, spark, config, tableName, showDataFrameLength, debugMode)
//      }
//    }

  }
}
