package com.ditas.utils

import com.ditas.configuration.ServerConfiguration

import java.io.FileInputStream


import com.ditas.ehealth.AvgRequest.{BloodTestComponentAverageGrpc, BloodTestComponentAverageRequest,
  BloodTestComponentAverageReply}
import com.ditas.ehealth.AllValuesRequest.{AllValuesForBloodTestComponentGrpc, AllValuesForBloodTestComponentRequest,
  AllValuesForBloodTestComponentReply}

import org.apache.spark.sql.Row
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object UtilFunctions {
  def loadServerConfig(filename : String): ServerConfiguration = {
    val yaml = new Yaml(new Constructor(classOf[ServerConfiguration]))
    val stream = new FileInputStream(filename)
    try {
      val obj = yaml.load(stream)
      obj.asInstanceOf[ServerConfiguration]
    } finally {
      stream.close()
    }
  }

  def anyNotNull(row: Row): Boolean = {
    val len = row.length

    var i = 0
    //skip patientId
    while (i < len) {
      if (!row.isNullAt(i)) {
        return true
      }
      i += 1
    }
    false
  }

}
