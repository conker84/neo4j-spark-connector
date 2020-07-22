package org.neo4j.spark

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.{AccessMode, AuthToken, AuthTokens, Config, SessionConfig}
import org.neo4j.driver.Config.TrustStrategy

class Neo4jOptions(private val parameters: java.util.Map[String, String]) extends Serializable {

  import Neo4jOptions._
  import QueryType._

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      return defaultValue
    }

    parameters.get(parameter).trim()
  }

  private val schemaFlattenLimit = getParameter(SCHEMA_FLATTEN_LIMIT, DEFAULT_SCHEMA_FLATTEN_LIMIT.toString).toInt

  val query: Neo4jQueryOptions = (
    getParameter(QUERY.toString.toLowerCase),
    getParameter(LABELS.toString.toLowerCase),
    getParameter(RELATIONSHIP.toString.toLowerCase())
  ) match {
    case (query, "", "") => Neo4jQueryOptions(QUERY, query, schemaFlattenLimit)
    case ("", label, "") => Neo4jQueryOptions(LABELS, label, schemaFlattenLimit)
    case ("", "", relationship) => Neo4jQueryOptions(RELATIONSHIP, relationship, schemaFlattenLimit)
    case _ => throw new IllegalArgumentException(
      s"You need to specify just one of these options: ${
        QueryType.values.toSeq.map(value => s"'${value.toString.toLowerCase()}'")
          .sorted.mkString(", ")
      }"
    )
  }

  val connection: Neo4jDriverOptions = Neo4jDriverOptions(
    getRequiredParameter(URL),
    getParameter(AUTH_TYPE, DEFAULT_AUTH_TYPE),
    getParameter(AUTH_BASIC_USERNAME, DEFAULT_EMPTY),
    getParameter(AUTH_BASIC_PASSWORD, DEFAULT_EMPTY),
    getParameter(AUTH_KERBEROS_TICKET, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_PRINCIPAL, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_CREDENTIALS, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_REALM, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_SCHEME, DEFAULT_EMPTY),
    getParameter(ENCRYPTION_ENABLED, DEFAULT_ENCRYPTION_ENABLED.toString).toBoolean,
    TrustStrategy.Strategy.valueOf(getParameter(ENCRYPTION_TRUST_STRATEGY, DEFAULT_ENCRYPTION_TRUST_STRATEGY.name())),
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH, DEFAULT_EMPTY),
    getParameter(CONNECTION_MAX_LIFETIME_MSECS, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_ACQUISITION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_LIVENESS_CHECK_TIMEOUT, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt
  )

  val session: Neo4jSessionOptions = Neo4jSessionOptions(
    getParameter(DATABASE, DEFAULT_EMPTY),
    AccessMode.valueOf(getParameter(ACCESS_MODE, DEFAULT_ACCESS_MODE.toString).toUpperCase())
  )
}

case class Neo4jQueryOptions(queryType: QueryType.Value, value: String, schemaFlattenLimit: Int) extends Serializable

case class Neo4jSessionOptions(database: String, accessMode: AccessMode = AccessMode.READ) {
  def toNeo4jSession: SessionConfig = {
    val builder = SessionConfig.builder()
      .withDefaultAccessMode(accessMode)

    if (database != null && database != "") {
      builder.withDatabase(database)
    }

    builder.build()
  }

}

case class Neo4jDriverOptions(
                               url: String,
                               auth: String,
                               username: String,
                               password: String,
                               ticket: String,
                               principal: String,
                               credentials: String,
                               realm: String,
                               schema: String,
                               encryption: Boolean,
                               trustStrategy: TrustStrategy.Strategy,
                               certificatePath: String,
                               lifetime: Int,
                               acquisitionTimeout: Int,
                               livenessCheckTimeout: Int,
                               connectionTimeout: Int
                             ) extends Serializable {

  def toDriverConfig: Config = {
    val builder = Config.builder()

    if (lifetime > -1) builder.withMaxConnectionLifetime(lifetime, TimeUnit.MILLISECONDS)
    if (acquisitionTimeout > -1) builder.withConnectionAcquisitionTimeout(acquisitionTimeout, TimeUnit.MILLISECONDS)
    if (livenessCheckTimeout > -1) builder.withConnectionLivenessCheckTimeout(livenessCheckTimeout, TimeUnit.MILLISECONDS)
    if (connectionTimeout > -1) builder.withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
    if (!encryption) {
      builder.withoutEncryption()
    }
    else {
      builder.withEncryption()
      builder.withTrustStrategy( trustStrategy match {
        case TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES => TrustStrategy.trustAllCertificates()
        case TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustSystemCertificates()
        case TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustCustomCertificateSignedBy(new File(certificatePath))
      })
    }

    builder.build()
  }

  def toNeo4jAuth: AuthToken = {
    auth match {
      case "basic" => AuthTokens.basic(username, password)
      case "none" => AuthTokens.none()
      case "kerberos" => AuthTokens.kerberos(ticket)
      case "custom" => AuthTokens.custom(principal, credentials, realm, schema)
      case _ => throw new IllegalArgumentException(s"Authentication method '${auth}' is not supported")
    }
  }
}

object Neo4jOptions {

  // connection options
  val URL = "url"

  // auth
  val AUTH_TYPE = "authentication.type" // basic, none, kerberos, custom
  val AUTH_BASIC_USERNAME = "authentication.basic.username"
  val AUTH_BASIC_PASSWORD = "authentication.basic.password"
  val AUTH_KERBEROS_TICKET = "authentication.kerberos.ticket"
  val AUTH_CUSTOM_PRINCIPAL = "authentication.custom.principal"
  val AUTH_CUSTOM_CREDENTIALS = "authentication.custom.credentials"
  val AUTH_CUSTOM_REALM = "authentication.custom.realm"
  val AUTH_CUSTOM_SCHEME = "authentication.custom.scheme"

  // driver
  val ENCRYPTION_ENABLED = "encryption.enabled"
  val ENCRYPTION_TRUST_STRATEGY = "encryption.trust.strategy"
  val ENCRYPTION_CA_CERTIFICATE_PATH = "encryption.ca.certificate.path"
  val CONNECTION_MAX_LIFETIME_MSECS = "connection.max.lifetime.msecs"
  val CONNECTION_LIVENESS_CHECK_TIMEOUT = "connection.liveness.timeout.msec"
  val CONNECTION_ACQUISITION_TIMEOUT_MSECS = "connection.acquisition.timeout.msecs"
  val CONNECTION_TIMEOUT_MSECS = "connection.timeout.msecs"

  // session options
  val DATABASE = "database"
  val ACCESS_MODE = "access.mode"

  // schema options
  val SCHEMA_FLATTEN_LIMIT = "schema.flatten.limit"

  // defaults
  val DEFAULT_EMPTY = ""
  val DEFAULT_TIMEOUT: Int = -1
  val DEFAULT_ACCESS_MODE = AccessMode.WRITE
  val DEFAULT_AUTH_TYPE = "basic"
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_ENCRYPTION_TRUST_STRATEGY = TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
  val DEFAULT_SCHEMA_FLATTEN_LIMIT = 10
}

object QueryType extends Enumeration {
  val QUERY, LABELS, RELATIONSHIP = Value
}