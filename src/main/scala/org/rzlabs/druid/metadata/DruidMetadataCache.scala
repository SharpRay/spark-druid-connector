package org.rzlabs.druid.metadata

import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.util.MyThreadUtils
import org.joda.time.Interval
import org.rzlabs.druid._
import org.rzlabs.druid.client._

import scala.collection.mutable.{Map => MMap}

case class DruidNode(name: String,
                     id: String,
                     address: String,
                     port: Int)

case class DruidClusterInfo(host: String,
                            curatorConnection: CuratorConnection,
                            serverStatus: ServerStatus,
                            druidDataSources: MMap[String, DruidDataSource])

trait DruidMetadataCache {

  def getDruidClusterInfo(druidRelationName: DruidRelationName,
                          options: DruidOptions): DruidClusterInfo

  def getDataSourceInfo(druidRelationName: DruidRelationName,
                        options: DruidOptions): DruidDataSource
}

trait DruidRelationInfoCache {

  self: DruidMetadataCache =>


  def buildColumnInfos(druidDataSource: DruidDataSource,
                       userSpecifiedColumnInfos: List[DruidRelationColumnInfo]
                       ): Map[String, DruidRelationColumn] = {
    val columns: Map[String, DruidColumn] = druidDataSource.columns

    def getDruidMetric(metricName: Option[String]): Option[DruidMetric] = {
      if (metricName.isDefined) {
        if (columns.contains(metricName.get) &&
            columns(metricName.get).isInstanceOf[DruidMetric]) {
          Some(columns(metricName.get).asInstanceOf[DruidMetric])
        } else None
      } else None
    }

    columns.map {
      case (columnName, druidColumn) =>
        val ci = userSpecifiedColumnInfos.find(_.column == columnName).getOrElse(null)
        val druidRelationColumn = if (ci != null) {
          val hllMetric = getDruidMetric(ci.hllMetric)
          val sketchMetric = getDruidMetric(ci.sketchMetric)
          DruidRelationColumn(columnName, druidColumn, hllMetric, sketchMetric)
        } else DruidRelationColumn(columnName, druidColumn)
        val cardinality: Option[Long] = if (druidColumn.isInstanceOf[DruidTimeDimension]) {
          Some(druidColumn.asInstanceOf[DruidTimeDimension].cardinality)
        } else if (druidColumn.isInstanceOf[DruidDimension]) {
          Some(druidColumn.asInstanceOf[DruidDimension].cardinality)
        } else if (druidColumn.isInstanceOf[DruidMetric]) {
          Some(druidColumn.asInstanceOf[DruidMetric].cardinality)
        } else None
        columnName -> druidRelationColumn.copy(cardinalityEstimate = cardinality)
    }
  }

  def druidRelation(dataSourceName: String,
                    timeDimensionCol: String,
                    userSpecifiedColumnInfos: List[DruidRelationColumnInfo],
                    options: DruidOptions): DruidRelationInfo = {

    val name = DruidRelationName(options.zkHost, dataSourceName)
    val druidDS = getDataSourceInfo(name, options)
    val columnInfos = buildColumnInfos(druidDS, userSpecifiedColumnInfos)
    val timeDimCol = if (druidDS.timestampSpec != null) {
      druidDS.timestampSpec.column
    } else if (timeDimensionCol != null) {
      timeDimensionCol
    } else {
      throw new DruidDataSourceException("The datasource time dimension should be specified.")
    }
    DruidRelationInfo(name, timeDimCol, columnInfos, options)
  }
}

object DruidMetadataCache extends DruidMetadataCache with MyLogging with DruidRelationInfoCache {

  private[metadata] val cache: MMap[String, DruidClusterInfo] = MMap() // zkHost -> DruidClusterInfo
  private val curatorConnections: MMap[String, CuratorConnection] = MMap()
  val threadPool = MyThreadUtils.newDaemonCachedThreadPool("druidZkEventExec", 10)

  private def curatorConnection(host: String, options: DruidOptions): CuratorConnection = {
    curatorConnections.getOrElse(host, {
      val cc = new CuratorConnection(host, options, cache, threadPool, (data: Array[Byte]) => ())
      curatorConnections(host) = cc
      cc
    })
  }

  def getDruidClusterInfo(druidRelationName: DruidRelationName,
                          options: DruidOptions): DruidClusterInfo = {
    cache.synchronized {
      if (cache.contains(druidRelationName.zkHost)) {
        cache(druidRelationName.zkHost)
      } else {
        val zkHost = druidRelationName.zkHost
        val cc = curatorConnection(zkHost, options)
        val coordClient = new DruidCoordinatorClient(cc.getCoordinator)
        val serverStatus = coordClient.serverStatus
        val druidClusterInfo = new DruidClusterInfo(zkHost, cc, serverStatus,
          MMap[String, DruidDataSource]())
        logInfo(s"Loading druid cluster info for $druidRelationName with zkHost $zkHost")
        druidClusterInfo
      }
    }
  }

  def getDataSourceInfo(druidRelationName: DruidRelationName,
                        options: DruidOptions): DruidDataSource = {
    val druidClusterInfo = getDruidClusterInfo(druidRelationName, options)
    val dataSourceName: String = druidRelationName.druidDataSource
    druidClusterInfo.synchronized {
      if (druidClusterInfo.druidDataSources.contains(dataSourceName)) {
        druidClusterInfo.druidDataSources(dataSourceName)
      } else {
        val broker: String = druidClusterInfo.curatorConnection.getBroker
        val brokerClient = new DruidQueryServerClient(broker, false)
        val druidDS = brokerClient.metadata(dataSourceName,
          options.loadMetadataFromAllSegments)
          .copy(druidVersion = druidClusterInfo.serverStatus.version)
        druidClusterInfo.druidDataSources(dataSourceName) = druidDS
        logInfo(s"Druid datasource info for ${dataSourceName} is loaded.")
        druidDS
      }
    }
  }
}