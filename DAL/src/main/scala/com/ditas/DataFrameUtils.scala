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
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory


object DataFrameUtils {
  private val LOGGER = LoggerFactory.getLogger("DataFrameUtils")

  def anyNotNull(row: Row, columnName: String = Constants.SUBJECT_ID_COL_NAME): Boolean = {
    val len = row.length
    var i = 0
    var fieldNames = row.schema.fieldNames
    //print patientId if its the only col
    if (len == 1 && fieldNames(0).equals(columnName))
      return true
    //skip patientId
    for( i <- 0 until len){
      if (!fieldNames(i).equals(columnName) && !row.isNullAt(i)) {
        return true
      }
    }
    false
  }


  def loadTableDFFromConfig(tableFilePrefix : String, spark: SparkSession, config: ServerConfiguration,
                            dataConfigName: String): DataFrame = {
    LOGGER.info("loadTableDFFromConfig")

    val connInfo = config.getDataTables.get(dataConfigName)
    val connTypeKey = dataConfigName+"_type"
    val connType = config.getDataTablesTypes.get(connTypeKey)
    if ((null == connInfo) || (null == connType)) {
      LOGGER.error("Configuration not found for data source: " + dataConfigName)
      return spark.emptyDataFrame
    }
    if (connType.equals("s3a")) {
      var dataDF: DataFrame = null
      dataDF = spark.read.parquet(connInfo)
      return dataDF
    } else if (connType.equals("jdbc")) {
      //Use jdbc connection:
      val url = config.getJdbcConfig.get("db.mysql.url")
      val user = config.getJdbcConfig.get("db.mysql.username")
      val pass = config.getJdbcConfig.get("db.mysql.password")
      var jdbcDF = spark.read.format("jdbc").option("url", url).option("dbtable", connInfo).
        option("user", user).option("password", pass).load
      return jdbcDF
    }
    LOGGER.error("unrecognized data frame connection type")
    spark.emptyDataFrame
  }


  def addTableToSpark (spark: SparkSession, config: ServerConfiguration,
                       dataConfigName: String, showDataFrameLength: Int, debugMode: Boolean) : Unit = {
    var tableDF = loadTableDFFromConfig(null, spark, config, dataConfigName)
    var sparkName = dataConfigName.toString()
    //There is an assumption that only one clauses table exists when executing the query returned from the engine.
    //The new query will contain clauses.column_name expression (for example, clauses.x1c9f199c)
    //It is because the engine's rules contains such clauses.column_name expression and the new query is generated
    //from the rules,
    if (dataConfigName.toString().contains(Constants.CLAUSES)) {
      sparkName = Constants.CLAUSES
    }
    tableDF.createOrReplaceTempView(sparkName)
    if (debugMode) {
      LOGGER.info("============= " + sparkName + " ===============")
      tableDF.distinct().show(showDataFrameLength, false)
    }
  }

  def writeToSparkTable (tableDF: DataFrame, spark: SparkSession, config: ServerConfiguration,
                         dataConfigName: String, showDataFrameLength: Int, debugMode: Boolean) : Unit = {
    var sparkName = dataConfigName.toString()
    LOGGER.info("writeToSparkTable")

    val connInfo = config.getDataTables.get(dataConfigName)
    val connTypeKey = dataConfigName+"_type"
    val connType = config.getDataTablesTypes.get(connTypeKey)
    if (connType.equals("s3a")) {
      tableDF.write.format("parquet").mode(SaveMode.Append).save(connInfo)
      LOGGER.info("s3a: " + connInfo)
    } else if (connType.equals("jdbc")) {
      //Use jdbc connection:
      val url = config.getJdbcConfig.get("db.mysql.url")
      val user = config.getJdbcConfig.get("db.mysql.username")
      val pass = config.getJdbcConfig.get("db.mysql.password")
      tableDF.write.format("jdbc").option("url", url).option("dbtable", connInfo).
        option("user", user).option("password", pass).save()
      LOGGER.info("jdbc: " + connInfo)
    } else {
      LOGGER.error("unrecognized data frame connection type")
      return
    }
    if (debugMode) {
      LOGGER.info("============= " + sparkName + " ===============")
      tableDF.distinct().show(showDataFrameLength, false)
    }

  }

}
