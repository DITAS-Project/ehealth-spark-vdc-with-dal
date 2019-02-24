package com.ditas

import com.ditas.configuration.ServerConfiguration


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

}
