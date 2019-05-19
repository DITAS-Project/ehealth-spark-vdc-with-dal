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


//  @ApiOperation(nickname = "getAllValuesForBloodTestComponent",
//    value = "Get timeseries of patient's blood test component",
//    notes =  "This method returns the collected values for a specific blood test component of a patient (identified " +
//      "by his SSN), to be used by medical doctors",
//    response = classOf[models.BloodTestComponentValue], responseContainer = "List", httpMethod = "GET")
//  @ApiResponses(Array(
//    new ApiResponse(code = 400, message = "Invalid parameters supplied"),
//    new ApiResponse(code = 500, message = "Error processing result")))
//  def getAllValuesForBloodTestComponent(@ApiParam(value = "The patient's SSN", required = true, allowMultiple = false) socialId: String,
//                                        @ApiParam(value = "The blood test component", required = true,
//                                          allowMultiple = false) testType: String)= Action.async {
//    implicit request =>
//      val spark = initService.getSparkSessionInstance
//      val patientSSN = socialId
//      var origtestType = testType
//
//      if (!request.headers.hasHeader("Purpose")) {
//        Future.successful(BadRequest("Missing purpose"))
//      } else if (!request.headers.hasHeader("RequesterId")) {
//        Future.successful(BadRequest("Missing RequesterId"))
//      } else if (dalURL.equals("")) {
//        Future.successful(BadRequest("Missing DAL url"))
//      } else{
//
//        val response:AllValuesForBloodTestComponentReply = EHealthClient.getAllValuesForBloodTestComponent(socialId, testType,
//          request.headers("Purpose"),
//          request.headers("RequesterId"), dalPort, dalURL)
//
//        if (response == "") {
//          Future.successful(InternalServerError("Error in enforcement engine"))
//        } else{
//          println (response)
//          Future.successful(Ok(JsonFormat.toJsonString(response)))
//        }
//      }
//  }


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
      val queryObject = request.body
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




              val queryObject = testType
              var avgTestType: String = null
              val todayDate = java.time.LocalDate.now
              val minBirthDate = todayDate.minusYears(endAgeRange)
              val maxBirthDate = todayDate.minusYears(startAgeRange)

//        if (testType.equals("cholesterol")) {
//          avgTestType = "avg(cholesterol_total_value)"
//        } else {
//          avgTestType = "avg(" + "%s_value".format(testType).replaceAll("\\.", "_") + ")"
//        }
//        val queryOnJoinTables = "SELECT " + avgTestType + " FROM joined where birthDate > \"" + minBirthDate + "\" AND birthDate < \"" + maxBirthDate + "\""

        if (testType.equals("cholesterol")) {
          avgTestType = "cholesterol_total_value"
              } else {
          avgTestType = "%s_value".format(testType)
              }
        val queryOnJoinTables = "SELECT patientId, date, %s FROM blood_tests".format(avgTestType)

        val response: EHealthQueryReply = EHealthClient.getBloodTestComponentAverage(queryOnJoinTables, Seq(), request.headers("authorization"), request.headers("Purpose"), dalPort, dalURL)

        if (response == "") {
          Future.successful(InternalServerError("Error in enforcement engine"))
        }
        else{
          if (debugMode) {
            println("Range: " + startAgeRange + " " + endAgeRange)
          }

          Future.successful(Ok(JsonFormat.toJsonString(response)))
        }
      }
  }
}





