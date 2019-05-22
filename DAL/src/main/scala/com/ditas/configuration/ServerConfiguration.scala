package com.ditas.configuration

import scala.beans.BeanProperty

class ServerConfiguration {
  @BeanProperty var debugMode: Boolean = false
  @BeanProperty var sparkAppName: String = null
  @BeanProperty var sparkHadoopF3S3AConfig = new java.util.HashMap[String, String]();
  @BeanProperty var jdbcConfig = new java.util.HashMap[String, String]();
  @BeanProperty var policyEnforcementUrl: String = null

  @BeanProperty var jwtServerTimeout: Int = 5000 // milliseconds
  @BeanProperty var jwksServerEndpoint: String = null
  @BeanProperty var jwksCheckServerCertificate: Boolean = true

  @BeanProperty var showDataFrameLength: Int = 10
  @BeanProperty var dataTables = new java.util.HashMap[String, String]();
  @BeanProperty var dataTablesTypes = new java.util.HashMap[String, String]();
  @BeanProperty var port: Integer = null

}
