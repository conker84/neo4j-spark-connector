package org.neo4j.spark

import java.util.{Optional, UUID}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.Neo4jDataSourceReader
import org.neo4j.spark.writer.Neo4jDataSourceWriter

class DataSource extends DataSourceV2 with ReadSupport with DataSourceRegister with WriteSupport {

  def createReader(options: DataSourceOptions) = new Neo4jDataSourceReader(options, UUID.randomUUID().toString)

  override def shortName(): String = "neo4j"

  override def createWriter(jobId: String,
                            structType: StructType,
                            saveMode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = saveMode match {
    case SaveMode.Overwrite | SaveMode.ErrorIfExists => Optional.of(new Neo4jDataSourceWriter(jobId, structType, saveMode, options))
    case _ => throw new UnsupportedOperationException("Supported Save Modes are SaveMode.Append and SaveMode.ErrorIfExists")
  }
}