package org.rzlabs.druid.metadata

import org.apache.spark.sql.MyLogging
import org.rzlabs.druid.{DruidDataSource, DruidQueryGranularity}

case class DruidOptions(zkHost: String,
                        zkSessionTimeoutMs: Int,
                        zkEnableCompression: Boolean,
                        zkQualifyDiscoveryNames: Boolean,
                        zkDruidPath: String,
                        poolMaxConnectionsPerRoute: Int,
                        poolMaxConnections: Int,
                        loadMetadataFromAllSegments: Boolean,
                        debugTransformations: Boolean,
                        timeZoneId: String,
                        useV2GroupByEngine: Boolean,
                        useSmile: Boolean,
                        queryGranularity: DruidQueryGranularity)

case class DruidRelationName(zkHost: String, druidDataSource: String)

case class DruidRelationInfo(fullName: DruidRelationName,
                             timeDimensionCol: String,
                             druidDataSource: DruidDataSource,
                             val druidColumns: Map[String, DruidRelationColumn],
                             val options: DruidOptions)

