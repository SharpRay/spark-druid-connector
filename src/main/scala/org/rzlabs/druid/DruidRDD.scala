package org.rzlabs.druid


import com.fasterxml.jackson.core.{Base64Variant, Base64Variants}
import org.apache.http.concurrent.Cancellable
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.rzlabs.druid.client.{CancellableHolder, ConnectionManager, DruidQueryServerClient, ResultRow}
import org.rzlabs.druid.metadata.{DruidMetadataCache, DruidRelationInfo}
import org.apache.spark.sql.sources.druid._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.concurrent.TrieMap

abstract class DruidPartition extends Partition {

  def queryClient(useSmile: Boolean, httpMaxConnPerRoute: Int,
                  httpMaxConnTotal: Int): DruidQueryServerClient

  def intervals: List[Interval]

  def setIntervalsOnQuerySpec(qrySpec: QuerySpec): QuerySpec = {
    qrySpec.setIntervals(intervals)
  }
}

case class BrokerPartition(idx: Int,
                           broker: String,
                           in: Interval) extends DruidPartition {

  override def index: Int = idx

  override def queryClient(useSmile: Boolean, httpMaxConnPerRoute: Int,
                           httpMaxConnTotal: Int): DruidQueryServerClient = {
    ConnectionManager.init(httpMaxConnPerRoute, httpMaxConnTotal)
    new DruidQueryServerClient(broker, useSmile)
  }

  override def intervals: List[Interval] = List(in)
}

class DruidRDD(sqlContext: SQLContext,
               drInfo: DruidRelationInfo,
               val druidQuery: DruidQuery
              ) extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  val httpMaxConnPerRoute = drInfo.options.poolMaxConnectionsPerRoute
  val httpMaxConnTotal = drInfo.options.poolMaxConnections
  val useSmile = druidQuery.useSmile
  val schema: StructType = druidQuery.schema(drInfo)

  // TODO: add recording Druid query logic.

  override def getPartitions: Array[Partition] = {
    val broker = DruidMetadataCache.getDruidClusterInfo(drInfo.fullName,
      drInfo.options).curatorConnection.getBroker
    druidQuery.intervals.zipWithIndex.map(t => new BrokerPartition(t._2, broker, t._1)).toArray
  }

  override def compute(split: Partition, context: TaskContext
                      ): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[DruidPartition]
    val qrySpec = partition.setIntervalsOnQuerySpec(druidQuery.qrySpec)
    log.info("Druid querySpec: " + Utils.toPrettyJson(Left(qrySpec)))


    var cancelCallback: TaskCancelHandler.TaskCancelHolder = null
    var resultIter: CloseableIterator[ResultRow] = null
    var client: DruidQueryServerClient = null
    val queryId = qrySpec.context.map(_.queryId).getOrElse(s"query-${System.nanoTime()}")
    var queryStartTime = System.currentTimeMillis()
    var queryStartDT = (new DateTime()).toString
    try {
      cancelCallback = TaskCancelHandler.registerQueryId(queryId, context)
      client = partition.queryClient(useSmile, httpMaxConnPerRoute, httpMaxConnTotal)
      client.setCancellableHolder(cancelCallback)
      queryStartTime = System.currentTimeMillis()
      queryStartDT = (new DateTime()).toString()
      resultIter = qrySpec.executeQuery(client)
    } catch {
      case _ if cancelCallback.wasCancelTriggered && client != null =>
        resultIter = new DummyResultIterator()
      case e: Throwable => throw e
    } finally {
      TaskCancelHandler.clearQueryId(queryId)
    }

    val druidExecTime = System.currentTimeMillis() - queryStartTime
    var numRows: Int = 0

    context.addTaskCompletionListener { taskContext =>
      // TODO: add Druid query metrics.
      resultIter.closeIfNeeded()
    }

    val rIter = new InterruptibleIterator[ResultRow](context, resultIter)
    val nameToTF: Map[String, String] = druidQuery.getValTFMap()

    rIter.map { r =>
      numRows += 1
      val row = new GenericInternalRow(schema.fields.map { field =>
        DruidValTransform.sparkValue(field, r.event(field.name),
          nameToTF.get(field.name), drInfo.options.timeZoneId)
      })
      row
    }
  }

}

/**
 * The "TaskCancel thread" tracks the Spark tasks that are executing Druid queries.
 * Periodically (current every 5 secs) it checks if any of the Spark tasks have been
 * cancelled and relays this to the current [[Cancellable]] associated with the
 * [[org.apache.http.client.methods.HttpExecutionAware]] connection handing the
 * Druid query.
 */
object TaskCancelHandler extends MyLogging {

  private val taskMap = TrieMap[String, (Cancellable, TaskCancelHolder, TaskContext)]()

  class TaskCancelHolder(val queryId: String, val taskContext: TaskContext) extends CancellableHolder {

    override def setCancellable(c: Cancellable): Unit = {
      log.debug(s"set cancellable for query $queryId")
      taskMap(queryId) = (c, this, taskContext)
    }

    @volatile var wasCancelTriggered: Boolean = false
  }

  def registerQueryId(queryId: String, taskContext: TaskContext): TaskCancelHolder = {
    log.debug(s"register query $queryId")
    new TaskCancelHolder(queryId, taskContext)
  }

  def clearQueryId(queryId: String): Unit = taskMap.remove(queryId)

  val sec5: Long = 5 * 1000

  object cancelCheckThread extends Runnable with MyLogging {

    def run(): Unit = {
      while (true) {
        Thread.sleep(sec5)
        log.debug("cancelThread woke up")
        var canceledTasks: Seq[String] = Seq() // queryId list
        taskMap.foreach {
          case (queryId, (request, taskConcelHolder, taskContext)) =>
            log.debug(s"checking task stageId = ${taskContext.stageId()}, " +
              s"partitionId = ${taskContext.partitionId()}, " +
              s"isInterrupted = ${taskContext.isInterrupted()}")
            if (taskContext.isInterrupted()) {
              try {
                taskConcelHolder.wasCancelTriggered = true
                request.cancel()
                log.info(s"aborted http request for query $queryId: $request")
                canceledTasks = canceledTasks :+ queryId
              } catch {
                case e: Throwable => log.warn(s"failed to abort http request: $request")
              }
            }
        }
        canceledTasks.foreach(clearQueryId)
      }
    }
  }

  val t = new Thread(cancelCheckThread)
  t.setName("DruidRDD-TaskCancelCheckThread")
  t.setDaemon(true)
  t.start()
}

object DruidValTransform {

  private[this] val toTSWithTZAdj = (druidVal: Any, tz: String) => {
    val dvLong = if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toLong
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toLong
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toLong
    } else if (druidVal.isInstanceOf[Integer]) {
      druidVal.asInstanceOf[Integer].toLong
    } else {
      druidVal
    }

    new DateTime(dvLong, DateTimeZone.forID(tz)).getMillis * 1000.asInstanceOf[SQLTimestamp]
  }

  private[this] val toTS = (druidVal: Any, tz: String) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].longValue()
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toLong
    } else if (druidVal.isInstanceOf[Integer]) {
      druidVal.asInstanceOf[Integer].toLong
    } else druidVal
  }

  private[this] val toString = (druidVal: Any, tz: String) => {
    UTF8String.fromString(druidVal.toString)
  }

  private[this] val toInt = (druidVal: Any, tz: String) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toInt
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toInt
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toInt
    } else if (druidVal.isInstanceOf[Integer]) {
      druidVal.asInstanceOf[Integer].toInt
    } else druidVal
  }

  private[this] val toLong = (druidVal: Any, tz: String) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toLong
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toLong
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toLong
    } else if (druidVal.isInstanceOf[Integer]) {
      druidVal.asInstanceOf[Integer].toLong
    } else druidVal
  }

  private[this] val toFloat = (druidVal: Any, tz: String) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toFloat
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toFloat
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toFloat
    } else if (druidVal.isInstanceOf[Integer]) {
      druidVal.asInstanceOf[Integer].toFloat
    } else druidVal
  }

  private[this] val tfMap: Map[String, (Any, String) => Any] = {
    Map[String, (Any, String) => Any](
      "toTSWithTZAdj" -> toTSWithTZAdj,
      "toTS" -> toTS,
      "toString" -> toString,
      "toInt" -> toInt,
      "toLong" -> toLong,
      "toFloat" -> toFloat
    )
  }

  def defaultValueConversion(f: StructField, druidVal: Any): Any = f.dataType match {
    case TimestampType if druidVal.isInstanceOf[Double] =>
      druidVal.asInstanceOf[Double].longValue()
    case StringType if druidVal != null => UTF8String.fromString(druidVal.toString)
    case LongType if druidVal.isInstanceOf[BigInt] =>
      druidVal.asInstanceOf[BigInt].longValue()
    case LongType if druidVal.isInstanceOf[Integer] =>
      druidVal.asInstanceOf[Integer].longValue()
    case BinaryType if druidVal.isInstanceOf[String] =>
      Base64Variants.getDefaultVariant.decode(druidVal.asInstanceOf[String])
    case _ => druidVal

  }

  def sparkValue(f: StructField, druidVal: Any, tfName: Option[String], tz: String): Any = {
    tfName match {
      case Some(tf) if (tfMap.contains(tf) && druidVal != null) => tfMap(tf)(druidVal, tz)
      case _ => defaultValueConversion(f, druidVal)
    }
  }

  def getTFName(sparkDT: DataType, adjForTZ: Boolean = false): String = sparkDT match {
    case TimestampType if adjForTZ => "toTSWithTZAdj"
    case TimestampType => "toTS"
    case StringType if !adjForTZ => "toString"
    case ShortType if !adjForTZ => "toInt"
    case LongType => "toLong"
    case FloatType => "toFloat"
    case _ => ""
  }


}

