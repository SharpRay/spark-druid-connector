package org.rzlabs.druid.metadata

import org.apache.spark.sql.MyLogging
import org.apache.spark.util.MyThreadUtils
import org.joda.time.Interval
import org.rzlabs.druid.DruidDataSource
import org.rzlabs.druid.client.CuratorConnection

import scala.collection.mutable.{Map => MMap}

case class DruidNode(name: String,
                     id: String,
                     address: String,
                     port: Int)

case class DruidClusterInfo(host: String,
                            curatorConnection: CuratorConnection,
                            druidDataSources: MMap[String, DruidDataSourceInfo])

case class DruidDataSourceInfo(druidDataSource: DruidDataSource,
                               timeBoundary: Interval)

object DruidMetadataCache extends MyLogging {

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
        val druidClusterInfo = new DruidClusterInfo(zkHost, cc, MMap[String, DruidDataSourceInfo]())
        logInfo(s"Loading druid cluster info for $druidRelationName with zkHost $zkHost")
        druidClusterInfo
      }
    }
  }

  def getDataSourceInfo(druidRelationName: DruidRelationName,
                        options: DruidOptions): DruidDataSourceInfo = {
    val druidClusterInfo = getDruidClusterInfo(druidRelationName, options)
    val druidDataSource: String = druidRelationName.druidDataSource
    druidClusterInfo.synchronized {
      if (druidClusterInfo.druidDataSources.contains(druidDataSource)) {
        druidClusterInfo.druidDataSources(druidDataSource)
      } else {
        val broker: String = druidClusterInfo.curatorConnection.getBroker

      }
    }
  }
}