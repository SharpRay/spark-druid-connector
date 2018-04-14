package org.rzlabs.druid.client

import java.io.IOException
import java.util.concurrent.ExecutorService

import org.apache.curator.framework.api.CompressionProvider
import org.apache.curator.framework.imps.GzipCompressionProvider
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths
import org.apache.spark.sql.MyLogging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rzlabs.druid.metadata.{DruidClusterInfo, DruidNode, DruidOptions}
import org.rzlabs.druid.{DruidDataSourceException, Utils}
import org.fasterxml.jackson.databind.ObjectMapper._

import scala.collection.mutable.{Map => MMap}

class CuratorConnection(val zkHost: String,
                        val options: DruidOptions,
                        val cache: MMap[String, DruidClusterInfo],
                        execSvc: ExecutorService,
                        updateTimeBoundary: String => Unit
                        ) extends MyLogging {

  // Cache the active historical servers.
  val serverQueueCacheMap: MMap[String, PathChildrenCache] = MMap()
  private val serverQueueCacheLock = new Object
  // serverName -> serverList
  var discoveryServers: MMap[String, Seq[String]] = MMap()
  private val discoveryCacheLock = new Object

  val announcementsPath = ZKPaths.makePath(options.zkDruidPath, "announcements")
  val serverSegmentsPath = ZKPaths.makePath(options.zkDruidPath, "segments")
  val discoveryPath = ZKPaths.makePath(options.zkDruidPath, "discovery")
  val loadQueuePath = ZKPaths.makePath(options.zkDruidPath, "loadQueue")
  val brokersPath = ZKPaths.makePath(discoveryPath, getServiceName("broker"))
  val coordinatorsPath = ZKPaths.makePath(discoveryPath, getServiceName("coordinator"))

  val framework: CuratorFramework = CuratorFrameworkFactory.builder
    .connectString(zkHost)
    .sessionTimeoutMs(options.zkSessionTimeoutMs)
    .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
    .compressionProvider(new PotentiallyGzippedCompressionProvider(options.zkEnableCompression))
    .build()

  val announcementsCache: PathChildrenCache = new PathChildrenCache(
    framework,
    announcementsPath,
    true,
    true,
    execSvc
  )

  val brokersCache: PathChildrenCache = new PathChildrenCache(
    framework,
    brokersPath,
    true,
    true,
    execSvc
  )

  val coordinatorsCache: PathChildrenCache = new PathChildrenCache(
    framework,
    coordinatorsPath,
    true,
    true,
    execSvc
  )

  /**
   * A [[PathChildrenCacheListener]] which is used to monitor
   * coordinators/brokers/overlords' in and out.
   * In the case of multiple brokers all them will be active if necessary.
   * In the case of multiple coordinators just one is active while others are standby.
   *
   */
  val discoveryListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case eventType @ (PathChildrenCacheEvent.Type.CHILD_ADDED |
             PathChildrenCacheEvent.Type.CHILD_REMOVED) =>
          discoveryCacheLock.synchronized {
            //val data = getZkDataForNode(event.getData.getPath)
            val data = event.getData.getData
            val druidNode = jsonMapper.readValue(new String(data), classOf[DruidNode])
            val host = s"${druidNode.address}:${druidNode.port}"
            val serviceName = druidNode.name
            val serverSeq = getServerSeq(serviceName)
            if (eventType == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              logInfo(s"Server[$serviceName][$host] is added in the path ${event.getData.getPath}")
              if (serverSeq.contains(host)) {
                logWarning(s"New server[$serviceName][$host] but there was already one, ignoring new one.", host)
              } else {
                discoveryServers(serviceName) = serverSeq :+ host
                logDebug(s"New server[$host] is added to cache.")
              }
            } else {
              logInfo(s"Server[$serviceName][$host] is removed from the path ${event.getData.getPath}")
              if (serverSeq.contains(host)) {
                discoveryServers(serviceName) = serverSeq.filterNot(_ == host)
                logDebug(s"Server[$host] is offline, so remove it from the cache.")
              } else {
                logError(s"Server[$host] is not in the cache, how to remove it from cache?")
              }
            }
          }
        case _ => ()
      }
    }
  }

  /**
   * A [[PathChildrenCacheListener]] which is used to monitor segments' in and out and
   * update time boundary for datasources.
   * The occurrence of a CHILD_ADDED event means there's a new segment of some datasource
   * is added to the `loadQueue`, we should do nothing because the segment may be queued
   * and it is not announced at that time.
   * The occurrence of a CHILD_REMOVED event means there's a segment is removed from `loadQueue`
   * and it will be announced then, we should update the datasource's time boundary.
   */
  val segmentLoadQueueListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case eventType @ PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          logDebug(s"event $eventType occurred.")
          // This will throw a KeeperException because the removed path is not existence.
          //val nodeData = getZkDataForNode(event.getData.getPath)
          val nodeData: Array[Byte] = event.getData.getData
          if (nodeData == null) {
            logWarning(s"Ignoring event: Type - ${event.getType}, " +
              s"Path - ${event.getData.getPath}, Version - ${event.getData.getStat.getVersion}")
          } else {
            updateTimeBoundary(new String(nodeData))
          }
        case _ => ()
      }
    }
  }

  /**
   * A [[PathChildrenCacheListener]] which is used to monitor historical servers' in and out
   * and manage the relationships of historical servers and their [[PathChildrenCache]]s.
   */
  val announcementsListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          // New historical server is added to the Druid cluster.
          serverQueueCacheLock.synchronized {
            // Get the historical server addr from path child data.
            val key = getServerKey(event)
            logInfo(s"Historical server[$key] is added to the path ${event.getData.getPath}")
            if (serverQueueCacheMap.contains(key)) {
              logWarning(s"New historical[$key] but there was already one, ignoring new one.")
            } else if (key != null) {
              val queuePath = ZKPaths.makePath(loadQueuePath, key)
              val queueCache = new PathChildrenCache(
                framework,
                queuePath,
                true,
                true,
                execSvc
              )
              queueCache.getListenable.addListener(segmentLoadQueueListener)
              serverQueueCacheMap(key) = queueCache
              logDebug(s"Starting inventory cache for $key, inventoryPath $queuePath", Array(key, queuePath))
              // Start cache and trigger the CHILD_ADDED event.
              //segmentsCache.start(StartMode.POST_INITIALIZED_EVENT)
              // Start cache and do not trigger the CHILD_ADDED by default.
              queueCache.start(StartMode.BUILD_INITIAL_CACHE)
            }
          }
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          // A historical server is offline.
          serverQueueCacheLock.synchronized {
            val key = getServerKey(event)
            logInfo(s"Historical server[$key] is removed from the path ${event.getData.getPath}")
            val segmentsCache: Option[PathChildrenCache] = serverQueueCacheMap.remove(key)
            if (segmentsCache.isDefined) {
              logInfo(s"Closing inventory for $key. Also removing listeners.")
              segmentsCache.get.getListenable.clear()
              segmentsCache.get.close()
            } else logWarning(s"Cache[$key] removed that wasn't cache!?")
          }
        case _ => ()
      }
    }
  }

  announcementsCache.getListenable.addListener(announcementsListener)
  brokersCache.getListenable.addListener(discoveryListener)
  coordinatorsCache.getListenable.addListener(discoveryListener)

  framework.start()
  announcementsCache.start(StartMode.POST_INITIALIZED_EVENT)
  brokersCache.start(StartMode.POST_INITIALIZED_EVENT)
  coordinatorsCache.start(StartMode.POST_INITIALIZED_EVENT)

//  def getService(name: String): String = {
//    getServices(name).head
//  }

  def getService(name: String): String = {
    val serviceName = getServiceName(name)
    discoveryCacheLock.synchronized {
      var serverSeq = getServerSeq(serviceName)
      if (serverSeq.isEmpty) {
        serverSeq = getServices(name)
        if (serverSeq.isEmpty) {
          return null
        } else {
          discoveryServers(serviceName) = serverSeq
        }
      }
      val server = serverSeq.head
      discoveryServers(serviceName) = serverSeq.tail :+ server
      server
    }
  }

  def getServices(name: String): Seq[String] = {

    val serviceName = getServiceName(name)
    val servicePath = ZKPaths.makePath(discoveryPath, serviceName)
    val childrenNodes: java.util.List[String] = framework.getChildren.forPath(servicePath)
    var services: Seq[String] = Nil
    try {
      val iter = childrenNodes.iterator()
      while (iter.hasNext) {
        val childNode = iter.next()
        val childPath = ZKPaths.makePath(servicePath, childNode)
        val data: Array[Byte] = getZkDataForNode(childPath)
        if (data != null) {
          val druidNode = jsonMapper.readValue(new String(data), classOf[DruidNode])
          services = services :+ s"${druidNode.address}:${druidNode.port}"
        }
      }
    } catch {
      case e: Exception =>
        throw new DruidDataSourceException(s"Failed to get '$name' for '$zkHost'", e)
    }
    if (services.isEmpty) {
      throw new DruidDataSourceException(s"There's no '$name' for 'zkHost' in path '$servicePath'")
    }
    services
  }

  def getBroker: String = {
    getService("broker")
  }

  def getCoordinator: String = {
    getService("coordinator")
  }

  private def getServerSeq(name: String): Seq[String] = {
    discoveryServers.get(name).getOrElse {
      val l: List[String] = List()
      discoveryServers(name) = l
      l
    }
  }

  private def getServiceName(name: String): String = {
    if (options.zkQualifyDiscoveryNames) {
      s"${options.zkDruidPath}:$name".tail
    } else name
  }

  private def getServerKey(event: PathChildrenCacheEvent): String = {
    val child: ChildData = event.getData
    //val data: Array[Byte] = getZkDataForNode(child.getPath)
    val data: Array[Byte] = child.getData
    if (data == null) {
      logWarning(s"Ignoring event: Type - ${event.getType}, " +
        s"Path - ${child.getPath}, Version - ${child.getStat.getVersion}")
      null
    } else {
      ZKPaths.getNodeFromPath(child.getPath)
    }
  }

  private def getZkDataForNode(path: String): Array[Byte] = {
    try {
      framework.getData.decompressed().forPath(path)
    } catch {
      case e: Exception => {
        logError(s"Exception occurs while getting data fro node $path", e)
        null
      }
    }
  }
}

/*
 * copied from druid code base.
 */
class PotentiallyGzippedCompressionProvider(val compressOutput: Boolean)
  extends CompressionProvider {

  private val base: GzipCompressionProvider = new GzipCompressionProvider


  @throws[Exception]
  def compress(path: String, data: Array[Byte]): Array[Byte] = {
    return if (compressOutput) base.compress(path, data)
    else data
  }

  @throws[Exception]
  def decompress(path: String, data: Array[Byte]): Array[Byte] = {
    try {
      return base.decompress(path, data)
    }
    catch {
      case e: IOException => {
        return data
      }
    }
  }
}