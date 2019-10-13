/**
 * Copyright 2018 IBM
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
package controllers


import java.io.FileInputStream

import org.apache.commons.lang3.StringUtils
import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import javax.inject.Inject
import org.apache.spark.sql.SparkSession
import play.api.Configuration
import javax.inject.Inject
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.mvc._

import scala.concurrent.Future
import bootstrap.Init
import io.swagger.annotations._
import scalapb.json4s.JsonFormat
import com.ditas.ehealth.EHealthService.EHealthQueryReply


// TODO thread pool!!!
@Api("EHealthVDCController")
class EHealthVDCController @Inject() (config: Configuration, initService: Init, ws: WSClient) extends InjectedController {
  private val LOGGER = LoggerFactory.getLogger("EHealthVDCController")
  var debugMode = initService.getDebugMode
  var showDataFrameLength = initService.getDfShowLen
  var dalURL = initService.getDalURL
  var dalPort: Int = initService.getDalPort


  @ApiOperation(nickname = "getAllValuesForBloodTestComponent",
    value = "Get timeseries of patient's blood test component",
    notes =  "This method returns the collected values for a specific blood test component of a patient (identified " +
      "by his SSN), to be used by medical doctors",
    response = classOf[models.BloodTestComponentValue], responseContainer = "List", httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid parameters supplied"),
    new ApiResponse(code = 500, message = "Error processing result")))
  def getAllValuesForBloodTestComponent(@ApiParam(value = "The patient's SSN", required = true, allowMultiple = false) socialId: String,
                                        @ApiParam(value = "The blood test component", required = true,
                                          allowMultiple = false) testType: String)= Action.async {
    implicit request =>
      val spark = initService.getSparkSessionInstance
      val patientSSN = socialId
      var origtestType = testType

      if (!request.headers.hasHeader("Purpose")) {
        Future.successful(BadRequest("Missing purpose"))
      } else if (dalURL.equals("")) {
        Future.successful(BadRequest("Missing DAL url"))
      }  else if (patientSSN.isEmpty || origtestType.isEmpty) {
        Future.successful(BadRequest("Missing patient social ID or test type"))
      } else {
        var flatTestType: String = null

        if (origtestType.equals("cholesterol")) {
          flatTestType = "cholesterol_total_value"
        } else {
          flatTestType = "%s_value".format(origtestType)
        }
        val queryOnJoinTables = "SELECT patientId, date, %s FROM blood_tests".format(flatTestType)
        try {
          val response: EHealthQueryReply = EHealthClient.query(queryOnJoinTables, Seq(), request.headers("authorization"), request.headers("Purpose"), dalPort, dalURL)

          if (null == response) {
            Future.successful(InternalServerError("Error in DAL or enforcement engine"))
          }
          else {
            if (debugMode) {
              println(s"Patient socialId: ${patientSSN}, testType: ${testType}")
            }

            Future.successful(Ok(JsonFormat.toJsonString(response)))
          }
        } catch {
          case ex: RuntimeException =>
            Future.successful(InternalServerError(ex.getMessage))
        }
      }
  }


  @ApiOperation(nickname = "getBloodTestComponentAverage",
    value = "Get average of component over an age range",
    notes =  "This method returns the average value for a specific blood test component in a specific age range, to be used by researchers. Since data are for researchers, patients' identifiers and quasi-identifiers won't be returned, making the output of this method anonymized.",
    response = classOf[models.ComponentAvg], httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid parameters supplied"),
    new ApiResponse(code = 500, message = "Error processing result")))
  def getBloodTestComponentAverage(@ApiParam(value = "The blood test component", required = true,
    allowMultiple = false) testType: String,
                                   @ApiParam(value = "Start age range", required = true,
                                     allowMultiple = false) startAgeRange: Int,
                                   @ApiParam(value = "End age range", required = true,
                                     allowMultiple = false) endAgeRange: Int) = Action.async {
    implicit request =>
      val spark = initService.getSparkSessionInstance
      val queryObject = testType
      var avgTestType:String = null
      val todayDate =  java.time.LocalDate.now
      val minBirthDate = todayDate.minusYears(endAgeRange)
      val maxBirthDate = todayDate.minusYears(startAgeRange)

      if (!request.headers.hasHeader("Purpose")) {
        Future.successful(BadRequest("Missing purpose"))
      } else if (dalURL.equals("")) {
        Future.successful(BadRequest("Missing DAL url"))
      }  else if (startAgeRange >= endAgeRange) {
        Future.successful(BadRequest("Wrong age range"))
      } else {
        if (testType.equals("cholesterol")) {
          avgTestType = "avg(cholesterol_total_value)"
        } else {
          avgTestType = "avg(" + "%s_value".format(testType).replaceAll("\\.", "_") + ")"
        }
//        // val queryOnJoinTables = "SELECT patientId, date, %s FROM blood_tests".format(flatTestType)
//        val queryOnJoinTables = "SELECT " + avgTestType + " FROM blood_tests where birthDate > \"" + minBirthDate + "\" AND birthDate < \"" + maxBirthDate + "\" FROM blood_tests"
        var flatTestType: String = null

        if (testType.equals("cholesterol")) {
          flatTestType = "cholesterol_total_value"
        } else {
          flatTestType = "%s_value".format(testType)
        }
        val queryOnJoinTables = "SELECT " + flatTestType + ", blood_tests.patientId FROM blood_tests INNER JOIN patientsProfiles ON blood_tests.patientId=patientsProfiles.patientId where " +
          "birthDate > \"" + minBirthDate + "\" AND birthDate < \"" + maxBirthDate + "\""

        val response: EHealthQueryReply = EHealthClient.query(queryOnJoinTables, Seq(), request.headers("authorization"), request.headers("Purpose"), dalPort, dalURL)

        if (response == "") {
          Future.successful(InternalServerError("Error in enforcement engine"))
        }
        else{
          if (debugMode) {
            println("Range: " + startAgeRange + " " + endAgeRange)
          }
          import spark.implicits._
          import org.apache.spark.sql.functions._
          val responseDF = response.values.toDF()
          if (debugMode) {
            println("DAL response: " + response.values.mkString(","))
            println("ResponseDF: " + responseDF.show(5))
          }
          val valuesDF = responseDF.select(json_tuple('value, flatTestType) as 'average_val)
          val averageDF = valuesDF.agg(avg("average_val") as 'value)
          if (debugMode) {
            println("Average: ")
            averageDF.show()
          }
          val resultStr = averageDF.toJSON.collect().head

          Future.successful(Ok(resultStr))
        }
      }
  }


  def getPatientData(socialId: String)= Action.async {
    implicit request =>
      val spark = initService.getSparkSessionInstance

      if (!request.headers.hasHeader("Purpose")) {
        Future.successful(BadRequest("Missing purpose"))
      } else if (dalURL.equals("")) {
        Future.successful(BadRequest("Missing DAL url"))
      }  else if (socialId.isEmpty ) {
        Future.successful(BadRequest("Missing patient ID "))
      } else {
          val queryOnJoinTables = "SELECT patientId, birthCity FROM patientsProfiles WHERE socialId='%s'".format(socialId)
        try {
          val response: EHealthQueryReply = EHealthClient.query(queryOnJoinTables, Seq(), request.headers("authorization"), request.headers("Purpose"), dalPort, dalURL)

          if (null == response) {
            Future.successful(InternalServerError("Error in DAL or enforcement engine"))
          }
          else {
              Future.successful(Ok(JsonFormat.toJsonString(response)))
          }
        } catch {
          case ex: RuntimeException =>
            Future.successful(InternalServerError(ex.getMessage))
        }
      }
  }
}





