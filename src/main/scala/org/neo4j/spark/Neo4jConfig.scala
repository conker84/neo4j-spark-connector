package org.neo4j.spark

import org.apache.spark.SparkConf
import org.neo4j.driver.{AuthToken, AuthTokens, Config, Driver, GraphDatabase, Session, SessionConfig}

/**
 * @author mh
 * @since 02.03.16
 */
case class Neo4jConfig(val url: String,
                       val user: String = "neo4j",
                       val password: Option[String] = None,
                       val database: Option[String] = None,
                       val encryptionStatus: Boolean) {

  private def boltConfig(): Config = if (encryptionStatus) Config.builder().withEncryption().build() else Config.builder().withoutEncryption().build()

  def driver(config: Neo4jConfig) : Driver = config.password match {
    case Some(pwd) => driver(config.url, AuthTokens.basic(config.user, pwd))
    case _ => driver(config.url, AuthTokens.none())
  }

  def driver(): Driver = driver(this)

  def driver(url: String, authToken: AuthToken): Driver = GraphDatabase.driver(url, authToken, boltConfig())

  def sessionConfig(): SessionConfig = database.map { SessionConfig.forDatabase(_) }.getOrElse(SessionConfig.defaultConfig())

}

object Neo4jConfig {
  val prefix = "spark.neo4j"
  def apply(sparkConf: SparkConf): Neo4jConfig = {
    val url = sparkConf.get(s"$prefix.bolt.url", "bolt://localhost")
    val user = sparkConf.get(s"$prefix.bolt.user", "neo4j")
    val password: Option[String] = sparkConf.getOption(s"$prefix.bolt.password")
    val database: Option[String] = sparkConf.getOption(s"$prefix.database")
    val encryptionStatus : Boolean = sparkConf.getBoolean(s"$prefix.bolt.encryption.status", defaultValue = false)
    Neo4jConfig(url, user, password, database, encryptionStatus)
  }
}
