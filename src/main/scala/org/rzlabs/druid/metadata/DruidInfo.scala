package org.rzlabs.druid.metadata

import org.rzlabs.druid.DruidQueryGranularity

case class DruidOptions(zkSessionTimeoutMs: Int,
                                zkEnableCompression: Boolean,
                                zkQualifyDiscoveryNames: Boolean,
                                zkDruidPath: String,
                                queryGranularity: DruidQueryGranularity)

case class DruidRelationName(zkHost: String, druidDataSource: String)