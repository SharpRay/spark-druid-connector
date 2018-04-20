package org.rzlabs.druid

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.rzlabs.DruidBaseModule
import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.fasterxml.jackson.databind.ObjectMapper._
import org.rzlabs.druid.metadata.{DruidMetadataCache, DruidOptions, DruidRelationColumnInfo, DruidRelationInfo}

import org.apache.spark.sql._

class DefaultSource extends RelationProvider with MyLogging {

  import DefaultSource._

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val dsName: String = parameters.getOrElse(DRUID_DS_NAME,
      throw new DruidDataSourceException(
        s"'$DRUID_DS_NAME' must be specified for Druid datasource.")
    )

    val timeDimensionCol: String = parameters.getOrElse(TIME_DIMENSION_COLUMN_NAME,
      null)

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

    val debugTransformations: Boolean = parameters.getOrElse(DEBUG_TRANSFORMATIONS,
      DEFAULT_DEBUG_TRANSFORMATIONS).toBoolean

    val queryGranularity = DruidQueryGranularity(
      parameters.getOrElse(QUERY_GRANULARITY, DEFAULT_QUERY_GRANULARITY))

    val druidOptions = DruidOptions(
      zkHost,
      zkSessionTimeout,
      zkEnableCompression,
      zKQualifyDiscoveryNames,
      zkDruidPath,
      poolMaxConnectionsPerRoute,
      poolMaxConnections,
      loadMetadataFromAllSegments,
      debugTransformations,
      queryGranularity
    )

    val druidRelationInfo: DruidRelationInfo =
      DruidMetadataCache.druidRelation(dsName,
        timeDimensionCol,
        hyperUniqueColumnInfos ++ sketchColumnInfos,
        druidOptions)

    val druidRelation = DruidRelation(druidRelationInfo)(sqlContext)

    addPhysicalRules(sqlContext, druidOptions)

    druidRelation
  }

  /**
   * There are 3 places to create a [[BaseRelation]] by calling
   * the `resolveRelation` methid of [[org.apache.spark.sql.execution.datasources.DataSource]]:
   *
   *   1. In the `run(sparkSession: SparkSession)` method in
   *      [[org.apache.spark.sql.execution.command.CreateDataSourceTableCommand]]
   *      when executing sql "create table using ...";
   *   2. In the `load(paths: String*)` method in [[org.apache.spark.sql.DataFrameReader]]
   *      when calling "spark.read.format(org.rzlabs.druid).load()";
   *   3. In the `load` method of the LoadingCache object "cachedDataSourceTables" in
   *      [[org.apache.spark.sql.hive.HiveMetastoreCatalog]] which called from the root
   *      method of `apply` in [[ResolveRelations]] in
   *      [[org.apache.spark.sql.catalyst.analysis.Analyzer]] which belongs to "resolution"
   *      batch in the logical plan analyzing phase of the executing sql "select ...".
   *
   * So we can add druid-related physical rules in the `resolveRelation` method in [[DefaultSource]].
   *
   * @param sqlContext
   * @param druidOptions
   */
  private def addPhysicalRules(sqlContext: SQLContext, druidOptions: DruidOptions) = {
    rulesLock.synchronized {
      if (!physicalRulesAdded) {
        sqlContext.sparkSession.experimental.extraStrategies ++= DruidBaseModule.physicalRules(druidOptions)
        physicalRulesAdded = true
      }
    }
  }

}

object DefaultSource {

  private val rulesLock = new Object

  private var physicalRulesAdded = false

  /**
   * Datasource name in Druid.
   */
  val DRUID_DS_NAME = "druidDatasource"

  /**
   * Time dimension name in a Druid datasource.
   */
  val TIME_DIMENSION_COLUMN_NAME = "timeDimensionColumn"

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
  val DEFAULT_CONN_POOL_MAX_CONNECTIONS = "100"

  val LOAD_METADATA_FROM_ALL_SEGMENTS = "loadMetadataFromAllSegments"
  val DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS = "true"

  val DEBUG_TRANSFORMATIONS = "debugTransformations"
  val DEFAULT_DEBUG_TRANSFORMATIONS = "false"

}
