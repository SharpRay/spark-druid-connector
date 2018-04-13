package org.rzlabs.druid.client

import java.io.InputStream

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.{HttpEntity, HttpHeaders}
import org.apache.http.client.methods._
import org.apache.spark.sql.MyLogging
import org.apache.http.concurrent._
import org.apache.http.entity.{ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.codehaus.jackson.map.ObjectMapper
import org.fasterxml.jackson.databind.ObjectMapper._
import org.joda.time.{DateTime, Interval}
import org.rzlabs.druid.{DruidDataSource, DruidDataSourceException}

import scala.util.Try

object ConnectionManager {

  @volatile private var initialized: Boolean = false

  lazy val pool = {
    val p = new PoolingHttpClientConnectionManager()
    p.setMaxTotal(40)
    p.setDefaultMaxPerRoute(8)
    p
  }
}

/**
 * A mechanism to relay [[org.apache.http.concurrent.Cancellable]] resources
 * associated with the "http connection" of a "DruidClient". This is used by
 * the [[TaskCancelHandler]] to capture the association between "Spark Tasks"
 * and "Cancellable" resources.
 */
trait CancellableHolder {
  def setCancellable(c: Cancellable)
}

/**
 * A mixin trait that relays [[Cancellable]] resources to
 * a [[CancellableHolder]].
 */
trait DruidClientHttpExecutionAware extends HttpExecutionAware {

  val ch: CancellableHolder

  abstract override def isAborted = super.isAborted

  abstract override def setCancellable(cancellable: Cancellable): Unit = {
    if (ch != null) {
      ch.setCancellable(cancellable)
    }
    super.setCancellable(cancellable)
  }
}

/**
 * Configure [[HttpPost]] to have the [[DruidClientHttpExecutionAware]] trait,
 * so that [[Cancellable]] resources are relayed to the registered [[CancellableHolder]].
 * @param url The url the request posted to.
 * @param ch The registered CancellableHolder.
 */
class DruidHttpPost(url: String, val ch: CancellableHolder)
  extends HttpPost(url) with DruidClientHttpExecutionAware

/**
 * Configure [[HttpGet]] to have the [[DruidClientHttpExecutionAware]] trait,
 * so that [[Cancellable]] resources are relayed to the registered [[CancellableHolder]].
 * @param url The url the request take data from.
 * @param ch The registered CancellableHolder.
 */
class DruidHttpGet(url: String, val ch: CancellableHolder)
  extends HttpGet(url) with DruidClientHttpExecutionAware

/**
 * `DruidClient` is not thread-safe because `cancellableHolder` state is used to relay
 * cancellable resources information.
 * @param host Server host.
 * @param port Server port.
 * @param useSmile Use smile binary JSON format or not.
 */
abstract class DruidClient(val host: String,
                           val port: Int,
                           val useSmile: Boolean = false) extends MyLogging {

  private var cancellableHolder: CancellableHolder = null

  def this(t: (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s: String) = {
    this(DruidClient.hostPort(s))
  }

  def setCancellableHolder(c: CancellableHolder): Unit = {
    cancellableHolder = c
  }

  /**
   * A [[CloseableHttpClient]] is a [[org.apache.http.client.HttpClient]]
   * with a `close` method in [[java.io.Closeable]].
   * @return
   */
  protected def httpClient: CloseableHttpClient = {
    val sTime = System.currentTimeMillis()
    val r = HttpClients.custom().setConnectionManager(ConnectionManager.pool).build()
    val eTime = System.currentTimeMillis()
    logDebug(s"Time to get httpClient: ${eTime - sTime}")
    logDebug("Pool Stats: {}", ConnectionManager.pool.getTotalStats)
    r
  }

  /**
   * Close the [[java.io.InputStream]] represented by the
   * `resp.getEntity.getContent()` to return a
   * [[org.apache.http.client.HttpClient]] to the
   * connection pool.
   * @param resp
   */
  protected def release(resp: CloseableHttpResponse): Unit = {
    Try {
      if (resp != null) EntityUtils.consume(resp.getEntity)
    } recover {
      case e => logError("Error returning client to pool",
        ExceptionUtils.getStackTrace(e))
    }
  }

  protected def getRequest(url: String) = new DruidHttpGet(url, cancellableHolder)
  protected def postRequest(url: String) = new DruidHttpPost(url, cancellableHolder)

  protected def addHeaders(req: HttpRequestBase, reqHeaders: Map[String, String]): Unit = {
    if (useSmile) {
      req.addHeader(HttpHeaders.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
    }
    if (reqHeaders != null) {
      reqHeaders.foreach(header => req.setHeader(header._1, header._2))
    }
  }

  @throws[DruidDataSourceException]
  protected def perform(url: String,
                        reqType: String => HttpRequestBase,
                        payload: ObjectNode,
                        reqHeaders: Map[String, String]): String = {
    var resp: CloseableHttpResponse = null

    val tis: Try[String] = for {
      r <- Try {
        val req: CloseableHttpClient = httpClient
        val request = reqType(url)
        // Just HttpPost extends HttpEntityEnclosingRequestBase.
        // HttpGet extends HttpRequestBase.
        if (payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          val input: HttpEntity = if (!useSmile) {
            new StringEntity(jsonMapper.writeValueAsString(payload), ContentType.APPLICATION_JSON)
          } else {
            new ByteArrayEntity(smileMapper.writeValueAsBytes(payload), null)
          }
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        resp = req.execute(request)
        resp
      }
      is <- Try {
        val status = r.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          if (r.getEntity != null) {
            IOUtils.toString(r.getEntity.getContent)
          } else {
            throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
          }
        } else {
          throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
        }
      }
    } yield is

    release(resp)
    tis.getOrElse(tis.failed.get match {
      case de: DruidDataSourceException => throw de
      case e => throw new DruidDataSourceException("Failed in communication with Druid", e)
    })
  }

  protected def post(url: String,
                     payload: ObjectNode,
                     reqHeaders: Map[String, String] = null): String = {
    perform(url, postRequest _, payload, reqHeaders)
  }

  protected def get(url: String,
                    payload: ObjectNode = null,
                    reqHeaders: Map[String, String] = null): String = {
    perform(url, getRequest _, payload, reqHeaders)
  }

  def timeBoundary(dataSource: String): Interval

  @throws[DruidDataSourceException]
  def metadata(url: String,
               dataSource: String,
               fullIndex: Boolean): DruidDataSource = {

    val in: Interval = timeBoundary(dataSource)
    // TODO: we do not fetch intervals of all segments for performence considerations.
    val ins: String =
      if (fullIndex) in.toString else in.withEnd(in.getStart.plusMillis(1)).toString

    val payload: ObjectNode = jsonMapper.createObjectNode()
      .put("queryType", "segmentMetadata")
      .put("dataSource", dataSource)
      .set("intervals", jsonMapper.createArrayNode()
        .add(ins)).asInstanceOf[ObjectNode]
      .set("analysisTypes", jsonMapper.createArrayNode()
        .add("cardinality")
        .add("interval")
        .add("minmax")
        .add("aggregators")
        .add("queryGranularity")
        .add("timestampSpec")).asInstanceOf[ObjectNode]
      .put("merge", "true")

    val resp: String = post(url, payload)
    logDebug(s"The json response of 'segmentMetadata' query: \n$resp")
    val lmr: List[MetadataResponse] =
      jsonMapper.readValue(resp, new TypeReference[List[MetadataResponse]] {})
    DruidDataSource(dataSource, lmr.head, List(in))
  }

  def serverStatus: ServerStatus = {
    val url = s"http://$host:$port/status"
    val is: String = get(url)
    jsonMapper.readValue(is, new TypeReference[ServerStatus] {})
  }
}


object DruidClient {

  val HOST = """([^:]*):(\d*)""".r

  def hostPort(s: String) : (String, Int) = {
    val HOST(h, p) = s
    (h, p.toInt)
  }
}

class DruidQueryServerClient(host: String, port: Int, useSmile: Boolean = false)
  extends DruidClient(host, port, useSmile) {

  @transient val url = s"http://$host:$port/druid/v2/?pretty"

  def this(t: (String, Int), useSmile: Boolean) = {
    this(t._1, t._2, useSmile)
  }

  def this(s: String, useSmile: Boolean) = {
    this(DruidClient.hostPort(s), useSmile)
  }

  @throws[DruidDataSourceException]
  override def timeBoundary(dataSource: String): Interval = {
    val payload: ObjectNode = jsonMapper.createObjectNode()
      .put("queryType", "timeBoundary")
      .put("dataSource", dataSource)
    val resp: String = post(url, payload)
    val objectNode = jsonMapper.readTree(resp)
    val maxTime: java.util.List[String] = objectNode.findValuesAsText("maxTime")
    val minTime: java.util.List[String] = objectNode.findValuesAsText("minTime")
    if (!maxTime.isEmpty && !minTime.isEmpty) {
      new Interval(
        new DateTime(minTime.get(0)),
        new DateTime(maxTime.get(0)).plusMillis(1)
      )
    } else {
      throw new DruidDataSourceException("Time boundary should include both the start time and the end time.")
    }
  }

  @throws[DruidDataSourceException]
  def metadata(dataSource: String, fullIndex: Boolean): DruidDataSource = {
    metadata(url, dataSource, fullIndex)
  }
}

class DruidCoordinatorClient(host: String, port: Int, useSmile: Boolean = false)
  extends DruidClient(host, port, useSmile) {

  @transient val urlPrefix = s"http://$host:$port/druid/coordinator/v1"

  def this(t: (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s: String) = {
    this(DruidClient.hostPort(s))
  }

  override def timeBoundary(dataSource: String): Interval = null
}
