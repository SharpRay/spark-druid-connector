package org.rzlabs.druid

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.fasterxml.jackson.databind.ObjectMapper._
import org.rzlabs.druid.metadata.{DruidRelationColumnInfo, DruidOptions}

class DefaultSource extends RelationProvider with MyLogging {

  import DefaultSource._

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val dsName: String = parameters.getOrElse(DRUID_DS_NAME,
      throw new DruidDataSourceException(
        s"'$DRUID_DS_NAME' must be specified for Druid datasource.")
    )

    val timeDimensionCol: String = parameters.getOrElse(TIME_DIMENSION_COLUMN_NAME,
      DEFAULT_TIME_DIMENSION_COLUMN_NAME)

    val hyperUniqueColumnInfos: List[DruidRelationColumnInfo] =
      parameters.get(HYPER_UNIQUE_COLUMN_INFO)
        .map(jsonMapper.readValue(_,
          new TypeReference[List[DruidRelationColumnInfo]] {})).getOrElse(List())

    val sketchColumnInfos: List[DruidRelationColumnInfo] =
      parameters.get(SKETCH_COLUMN_INFO)
        .map(jsonMapper.readValue(_,
          new TypeReference[List[DruidRelationColumnInfo]] {})).getOrElse(List())

    val zkHost: String = parameters.getOrElse(ZK_HOST, DEFAULT_ZK_HOST)

    val zkDruidPath: String = parameters.getOrElse(ZK_DRUID_PATH, DEFAULT_ZK_DRUID_PATH)

    val zkSessionTimeout: Int = parameters.getOrElse(ZK_SESSION_TIMEOUT,
      DEFAULT_ZK_SESSION_TIMEOUT).toInt

    val zkEnableCompression: Boolean = parameters.getOrElse(ZK_ENABLE_COMPRESSION,
      DEFAULT_ZK_ENABLE_COMPRESSION).toBoolean

    val zKQualifyDiscoveryNames: Boolean = parameters.getOrElse(ZK_QUALIFY_DISCOVERY_NAMES,
      DEFAULT_ZK_QUALIFY_DISCOVERY_NAMES).toBoolean

    val poolMaxConnectionsPerRoute: Int = parameters.getOrElse(CONN_POOL_MAX_CONNECTIONS_PER_ROUTE,
      DEFAULT_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE).toInt

    val poolMaxConnections: Int = parameters.getOrElse(CONN_POOL_MAX_CONNECTIONS,
      DEFAULT_CONN_POOL_MAX_CONNECTIONS).toInt

    val loadMetadataFromAllSegments: Boolean = parameters.getOrElse(LOAD_METADATA_FROM_ALL_SEGMENTS,
      DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS).toBoolean

    val queryGranularity = DruidQueryGranularity(
      parameters.getOrElse(QUERY_GRANULARITY, DEFAULT_QUERY_GRANULARITY))

    val druidRelationOptions = DruidOptions(
      zkHost,
      zkSessionTimeout,
      zkEnableCompression,
      zKQualifyDiscoveryNames,
      zkDruidPath,
      poolMaxConnectionsPerRoute,
      poolMaxConnections,
      loadMetadataFromAllSegments,
      queryGranularity
    )


  }
}

object DefaultSource {

  /**
   * Datasource name in Druid.
   */
  val DRUID_DS_NAME = "druidDatasource"

  /**
   * Time dimension name in a Druid datasource.
   */
  val TIME_DIMENSION_COLUMN_NAME = "timeDimensionColumn"
  val DEFAULT_TIME_DIMENSION_COLUMN_NAME = "__time"

  val HYPER_UNIQUE_COLUMN_INFO = "hyperUniqueColumnInfos"

  val SKETCH_COLUMN_INFO ="sketchColumnInfos"

  /**
   * Zookeeper server host name with pattern "host:port".
   */
  val ZK_HOST = "zkHost"
  val DEFAULT_ZK_HOST = "localhost"

  val ZK_SESSION_TIMEOUT = "zkSessionTimeoutMs"
  val DEFAULT_ZK_SESSION_TIMEOUT = "30000"

  val ZK_ENABLE_COMPRESSION ="zkEnableCompression"
  val DEFAULT_ZK_ENABLE_COMPRESSION = "true"

  /**
   * Druid cluster sync path on zk.
   */
  val ZK_DRUID_PATH = "zkDruidPath"
  val DEFAULT_ZK_DRUID_PATH = "/druid"

  val ZK_QUALIFY_DISCOVERY_NAMES = "zkQualifyDiscoveryNames"
  val DEFAULT_ZK_QUALIFY_DISCOVERY_NAMES = "true"

  /**
   * The query granularity that should be told Druid.
   * The options include 'minute', 'hour', 'day' and etc.
   */
  val QUERY_GRANULARITY = "queryGranularity"
  val DEFAULT_QUERY_GRANULARITY = "none"

  /**
   * The max simultaneous live connections per Druid server.
   */
  val CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = "maxConnectionsPerRoute"
  val DEFAULT_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = "20"

  /**
   * The max simultaneous live connections of the Druid cluster.
   */
  val CONN_POOL_MAX_CONNECTIONS = "maxConnections"
  val DEFAULT_CONN_POOL_MAX_CONNECTIONS = "100""

  val LOAD_METADATA_FROM_ALL_SEGMENTS = "loadMetadataFromAllSegments"
  val DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS = "true"

}
