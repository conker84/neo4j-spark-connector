package org.neo4j.spark.writer

import java.util
import java.util.Collections

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Session, Transaction, Values}
import org.neo4j.spark.service.MappingService
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{DriverCache, Neo4jOptions, QueryType}

class Neo4jDataWriter(jobId: String,
                           structType: StructType,
                           saveMode: SaveMode,
                           options: Neo4jOptions) extends DataWriter[InternalRow] with Logging {

  private var session: Session = _
  private var transaction: Transaction = _

  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private val mappingService = new MappingService(options)

  private val batch: util.List[java.util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()

  val query: String = createStatementForNodes()

  override def write(record: InternalRow): Unit = {
    if (session == null) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession)
      transaction = session.beginTransaction()
    }

    batch.add(mappingService.toParameter(record, structType))

    if (batch.size() == options.nodeMetadata.batchSize) {
      writeBatch
    }
  }

  private def writeBatch = {
    log.info(s"Writing a batch of ${batch.size()} elements to Neo4j")
    if (log.isDebugEnabled) {
      log.info(s"Writing batch into Neo4j with query: $query")
    }
    val result = transaction.run(query, Values.value(Collections.singletonMap[String, Object]("events", batch)))
    if (log.isDebugEnabled) {
      val summary = result.consume()
      val counters = summary.counters()
      log.debug(
        s"""Batch saved into Neo4j data with:
           | - nodes created: ${counters.nodesCreated()}
           | - nodes deleted: ${counters.nodesDeleted()}
           | - relationships created: ${counters.relationshipsCreated()}
           | - relationships deleted: ${counters.relationshipsDeleted()}
           | - properties set: ${counters.propertiesSet()}
           | - labels added: ${counters.labelsAdded()}
           | - labels removed: ${counters.labelsRemoved()}
           |""".stripMargin)
    }
    batch.clear()
  }

  private def createStatementForNodes(): String = {
    val keyword = saveMode match {
      case SaveMode.Overwrite => "MERGE"
      case SaveMode.ErrorIfExists => "CREATE"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }
    val labels = options.nodeMetadata.labels.map(l => s"`$l`").mkString(":")
    val keys = options.nodeMetadata.nodeKeys.values.map(k => s"`$k`").map(k => s"$k: event.keys.$k").mkString(", ")
    val query = s"""UNWIND ${"$"}events AS event
       |$keyword (node${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
       |SET node += event.properties
       |""".stripMargin
    query
  }

  override def commit(): WriterCommitMessage = {
    writeBatch
    transaction.commit()
    close
    null
  }

  override def abort(): Unit = {
    transaction.rollback()
    close
    Unit
  }

  private def close = {
    Neo4jUtil.closeSafety(transaction)
    Neo4jUtil.closeSafety(session)
  }
}
