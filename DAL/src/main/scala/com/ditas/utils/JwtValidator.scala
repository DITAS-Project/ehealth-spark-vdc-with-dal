package com.ditas.utils

import java.io.{IOException, InputStream}
import java.net.URL
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.security.spec.RSAPublicKeySpec
import java.security.{PublicKey, SecureRandom, cert}
import java.util
import java.util.logging.Logger

import com.auth0.jwk.{GuavaCachedJwkProvider, SigningKeyNotFoundException, UrlJwkProvider}
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{DecodedJWT, RSAKeyProvider}
import com.ditas.configuration.ServerConfiguration
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import javax.net.ssl._
import org.apache.commons.codec.binary.Base64
import org.apache.http.conn.ssl.NoopHostnameVerifier
import pdi.jwt.Jwt
import pdi.jwt.exceptions.JwtValidationException

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}


class JwtValidator(serverConfigFile: ServerConfiguration) {
  private val LOGGER = Logger.getLogger(getClass.getName)
  private val AUTH_HEADER_PATTERN = "Bearer (.*)".r

  private val JWKS_ENDPOINT = new URL(serverConfigFile.jwksServerEndpoint)
  val provider = new GuavaCachedJwkProvider(new UrlJwkProvider(JWKS_ENDPOINT))
  val keyProvider = new RSAKeyProvider() {
    override def getPublicKeyById(kid: String): RSAPublicKey = {
      val jwk = provider.get(kid)
      jwk.getPublicKey.asInstanceOf[RSAPublicKey]
    }

    override def getPrivateKey: RSAPrivateKey = null

    override def getPrivateKeyId: String = null
  }

  val algorithm = Algorithm.RSA256(keyProvider)
  val checkCertificate: Boolean = serverConfigFile.jwksCheckServerCertificate

  def validateJwtToken(authorizationHeader: String, jwtServerTimeout: Int, validRoles: mutable.Buffer[String]): String = {
    val token = for (m <- AUTH_HEADER_PATTERN.findFirstMatchIn(authorizationHeader)) yield m.group(1)

    val tokenStr = token.getOrElse("")

    var decodedJWT :DecodedJWT = null
    if (checkCertificate) {
      decodedJWT = JWT.require(algorithm).build().verify(tokenStr)
    } else {
      val signingKey = getSigningKeys(jwtServerTimeout)
      val kid = signingKey._1
      LOGGER.info("Using Keycloak public key: " + kid)
      val publicKey = signingKey._2
      Jwt.validate(tokenStr, publicKey)
      decodedJWT = new JWT().decodeJwt(tokenStr)
    }
    validateRoles(validRoles, decodedJWT)
    decodedJWT.getPayload
  }


  private def validateRoles(validRoles: mutable.Buffer[String], decodedJWT: DecodedJWT): Unit = {
    val roles = decodedJWT.getClaim("realm_access")
    if (roles.isNull) {
      throw new JwtValidationException("realm_access and roles not found in token");
    }
    if (validRoles.contains("*")) {
      LOGGER.warning("Not doing role checking. All Roles are acceptable.")
      return
    }
    val rolesList = roles.asMap().get("roles")
    if (null == rolesList) {
      throw new JwtValidationException("realm_access and roles not found in token");
    }
    val rolesArrayList = rolesList.asInstanceOf[util.ArrayList[String]]
    LOGGER.info("client roles: " + rolesList)
    val matchingRoles = validRoles.intersect(rolesArrayList.asScala)
    if (matchingRoles.isEmpty) {
      throw new JwtValidationException("Role not valid")
    }
    LOGGER.info("Matching roles: " + matchingRoles.mkString(","))
  }

  private def getSigningKeys(jwtServerTimeout: Int): (String, PublicKey) = {
    val jwks = getJwks(JWKS_ENDPOINT, false, "myKeystore", jwtServerTimeout)
    if (jwks("keys").isEmpty) {
      throw new JwtValidationException("No keys received from JWKS")
    }
    LOGGER.info(jwks.mkString(","))
    val key = jwks("keys")(0)
    if ((key("use") != "sig") ||
      (key("kty") != "RSA") ||
      key("kid").isEmpty ||
      key("n").isEmpty ||
      key("e").isEmpty) {
      throw new JwtValidationException("Incorrect keys received from JWKS")
    }
    val publicKey = getPEMFromRSA(key("n"), key("e"))
    val kid = key("kid")
    LOGGER.info("Using Keycloak public key: " + kid)
    (kid, publicKey)
  }

  private def getJwks(url: URL, checkCertificate: Boolean, keyStoreFileName: String, jwtServerTimeout: Int): Map[String, List[Map[String, String]]] = { // Map[String, AnyRef]
    var inputStream: Option[InputStream] = None
    try {
      val c = url.openConnection.asInstanceOf[HttpsURLConnection]

      LOGGER.warning("Warning ! Https connections will be done without checking certificate. Do not use in production.")
      val sslContext: SSLContext = {
        val permissiveTrustManager: TrustManager = new X509TrustManager() {
          override def checkClientTrusted(x509Certificates: Array[cert.X509Certificate], s: String): Unit = {}

          override def checkServerTrusted(x509Certificates: Array[cert.X509Certificate], s: String): Unit = {}

          override def getAcceptedIssuers: Array[cert.X509Certificate] = Array.empty
        }
        val ctx = SSLContext.getInstance("TLS")
        ctx.init(Array.empty, Array(permissiveTrustManager), new SecureRandom())
        ctx
      }
      val hostnameVerifier = NoopHostnameVerifier.INSTANCE
      c.setSSLSocketFactory(sslContext.getSocketFactory)
      c.setHostnameVerifier(hostnameVerifier)

      c.setConnectTimeout(jwtServerTimeout)
      c.setReadTimeout(jwtServerTimeout)
      inputStream = Option(c.getInputStream)

      val content = scala.io.Source.fromInputStream(inputStream.get).getLines.mkString

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val valuesMap = mapper.readValue(content, classOf[Map[String, List[Map[String, String]]]])

      return valuesMap
    } catch {
      case e: IOException =>
        throw new SigningKeyNotFoundException("Cannot obtain jwks from url " + url.toString, e)
    } finally {
      if (inputStream.isDefined) {
        inputStream.get.close()
      }
    }
  }

  private def getPEMFromRSA(modulusStr: String, exponentStr: String): PublicKey = {
    val modulus = BigInt(1, Base64.decodeBase64(modulusStr))
    val exponent = BigInt(1, Base64.decodeBase64(exponentStr))

    val publicKeySpec = new RSAPublicKeySpec(modulus.bigInteger, exponent.bigInteger)
    val publicKey = java.security.KeyFactory.getInstance("RSA").generatePublic(publicKeySpec)
    publicKey
  }
}


