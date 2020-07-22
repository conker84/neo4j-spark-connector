package org.neo4j.spark.service

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructField, StructType}
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.spark.{Neo4jOptions, QueryType}
import org.neo4j.spark.util.Neo4jUtil

class MappingService(private val options: Neo4jOptions) {

  private def toQuery(record: InternalRow, structType: StructType): java.util.Map[String, Object] = {
    throw new UnsupportedOperationException("TODO implement the method")
  }

  private def toRelationship(record: InternalRow, structType: StructType): java.util.Map[String, Object] = {
    throw new UnsupportedOperationException("TODO implement the method")
  }

  private def toNode(row: InternalRow, schema: StructType): java.util.Map[String, Object] = {
    val rowMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val keys: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val properties: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    rowMap.put("keys", keys)
    rowMap.put("properties", properties)

    val seq = row.toSeq(schema)
    for (i <- 0 to schema.size - 1) {
      val field = schema(i)
      val neo4jValue = Neo4jUtil.convertFromSpark(seq(i), field)
      neo4jValue match {
        case map: MapValue => {
          Neo4jUtil.flattenMap(map.asMap(), field.name).forEach((key, value) => {
            properties.put(options.nodeMetadata.nodeMappings.get(key).getOrElse(key), Values.value(value))
          })
        }
        case _ => {
          if (options.nodeMetadata.nodeKeys.contains(field.name)) {
            keys.put(options.nodeMetadata.nodeKeys.get(field.name).get, neo4jValue)
          } else {
            properties.put(options.nodeMetadata.nodeMappings.get(field.name).getOrElse(field.name), neo4jValue)
          }
        }
      }
    }

    rowMap
  }

  def toParameter(row: InternalRow, schema: StructType): java.util.Map[String, Object] = options.query.queryType match {
    case QueryType.LABELS => toNode(row, schema)
    case QueryType.RELATIONSHIP => toRelationship(row, schema)
    case QueryType.QUERY => toQuery(row, schema)
  }

}
