package org.rzlabs.druid.metadata

import org.apache.spark.sql.MyLogging
import org.rzlabs.druid.DruidQueryGranularity

case class DruidOptions(zkHost: String,
                        zkSessionTimeoutMs: Int,
                        zkEnableCompression: Boolean,
                        zkQualifyDiscoveryNames: Boolean,
                        zkDruidPath: String,
                        poolMaxConnectionsPerRoute: Int,
                        poolMaxConnections: Int,
                        loadMetadataFromAllSegments: Boolean,
                        queryGranularity: DruidQueryGranularity)

case class DruidRelationName(zkHost: String, druidDataSource: String)

case class DruidRelationInfo(fullName: DruidRelationName,
                             timeDimensionCol: String,
                             val druidColumn: Map[String, DruidRelationColumn],
                             val options: DruidOptions)

