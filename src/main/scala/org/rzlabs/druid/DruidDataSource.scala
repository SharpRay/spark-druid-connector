package org.rzlabs.druid

import com.clearspring.analytics.stream.cardinality.ICardinality
import org.apache.spark.sql.types._
import org.joda.time.Interval
import org.rzlabs.druid.client.{Aggregator, ColumnDetail, MetadataResponse, TimestampSpec}

/**
 * Driud data type enum. All the value name are from the
 * `type` field of the column JSON by calling the `segmentMetadata` API.
 */
object DruidDataType extends Enumeration {
  val String = Value("STRING")
  val Long = Value("LONG")
  val Float = Value("FLOAT")
  val HyperUnique = Value("hyperUnique")
  val ThetaSketch = Value("thetaSketch")

  def sparkDataType(t: String): DataType = sparkDataType(DruidDataType.withName(t))

  def sparkDataType(t: DruidDataType.Value): DataType = t match {
    case String => StringType
    case Long => LongType
    case Float => FloatType
    case HyperUnique => BinaryType
    case ThetaSketch => BinaryType
  }
}

sealed trait DruidColumn {
  val name: String
  val dataType: DruidDataType.Value
  val size: Long // in bytes

  /**
   * Come from the segment metadata query.
   * Just for time and dimension fields.
   */
  val cardinality: Long

  def isDimension(excludeTime: Boolean = false): Boolean
}

object DruidColumn {

  def apply(name: String,
            c: ColumnDetail,
            numRows: Long,
            timeTicks: Long): DruidColumn = {

    if (name == DruidDataSource.INNER_TIME_COLUMN_NAME) {
      DruidTimeDimension(name, DruidDataType.withName(c.`type`), c.size, Math.min(numRows, timeTicks))
    } else if (c.cardinality.isDefined) {
      DruidDimension(name, DruidDataType.withName(c.`type`), c.size, c.cardinality.get)
    } else {
      // The metric's cardinality is considered the same as the datasource row number.
      DruidMetric(name, DruidDataType.withName(c.`type`), c.size, numRows)
    }
  }
}

case class DruidDimension(name: String,
                          dataType: DruidDataType.Value,
                          size: Long,
                          cardinality: Long) extends DruidColumn {
  def isDimension(excludeTime: Boolean = false) = true
}

case class DruidTimeDimension(name: String,
                              dataType: DruidDataType.Value,
                              size: Long,
                              cardinality: Long) extends DruidColumn {
  def isDimension(excludeTime: Boolean = false): Boolean = !excludeTime
}

case class DruidMetric(name: String,
                       dataType: DruidDataType.Value,
                       size: Long,
                       cardinality: Long) extends DruidColumn {
  def isDimension(excludeTime: Boolean) = false
}

trait DruidDataSourceCapability {
  def druidVersion: String
  def supportsBoundFilter: Boolean = false
}

object DruidDataSourceCapability {
  private def newerVersion(druidVersion: String, oldVersion: String): Boolean = {
    druidVersion.split("\\.").zip(oldVersion.split("\\.")).forall {
      case (v1, v2) => v1.toInt >= v2.toInt
    }
  }
  def supportsBoundFilter(druidVersion: String): Boolean = newerVersion(druidVersion, "0.9.0")
  def supportsQueryGranularityMetadata(druidVersion: String): Boolean = newerVersion(druidVersion, "0.9.1")
  def supportsTimestampSpecMetadata(druidVersion: String): Boolean = newerVersion(druidVersion, "0.9.2")
}

case class DruidDataSource(name: String,
                           var intervals: List[Interval],
                           columns: Map[String, DruidColumn],
                           size: Long,
                           numRows: Long,
                           timeTicks: Long,
                           aggregators: Option[Map[String, Aggregator]] = None,
                           timestampSpec: Option[TimestampSpec] = None,
                           druidVersion: String = null) extends DruidDataSourceCapability {

  import DruidDataSource._

  lazy val timeDimension: Option[DruidColumn] = columns.values.find {
    case c if c.name == INNER_TIME_COLUMN_NAME => true
    case c if timestampSpec.isDefined && c.name == timestampSpec.get.column => true
    case _ => false
  }

  lazy val dimensions: IndexedSeq[DruidDimension] = columns.values.filter {
    case d: DruidDimension => true
    case _ => false
  }.map{_.asInstanceOf[DruidDimension]}.toIndexedSeq

  lazy val metrics: Map[String, DruidMetric] = columns.values.filter {
    case m: DruidMetric => true
    case _ => false
  }.map(m => m.name -> m.asInstanceOf[DruidMetric]).toMap

  override def supportsBoundFilter: Boolean = DruidDataSourceCapability.supportsBoundFilter(druidVersion)

  def numDimensions = dimensions.size

  def indexOfDimension(d: String): Int = {
    dimensions.indexWhere(_.name == d)
  }

  def metric(name: String): Option[DruidMetric] = metrics.get(name)

  def timeDimensionColName(timeDimCol: String) = {
    if (timestampSpec.nonEmpty) {
      timestampSpec.get.column
    } else if (timeDimCol != null) {
      timeDimCol
    } else {
      DruidDataSource.INNER_TIME_COLUMN_NAME
    }
  }
}

object DruidDataSource {

  val INNER_TIME_COLUMN_NAME = "__time"
  val TIMESTAMP_KEY_NAME = "timestamp"

  def apply(dataSource: String, mr: MetadataResponse,
            ins: List[Interval]): DruidDataSource = {

    val numRows = mr.getNumRows
    // TODO just 1 interval in 'ins' List and it's the
    // time boundary of the datasource. So the time ticks may not be correct.
    val timeTicks = mr.timeTicks(ins)

    val columns = mr.columns.map {
      case (name, columnDetail) =>
        name -> DruidColumn(name, columnDetail, numRows, timeTicks)
    }
    new DruidDataSource(dataSource, ins, columns, mr.size, numRows, timeTicks,
      mr.aggregators, mr.timestampSpec)
  }
}
