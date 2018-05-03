package org.rzlabs.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.rzlabs.druid.metadata.{DruidRelationColumn, DruidRelationInfo}

import scala.collection.mutable.ArrayBuffer

case class DruidRelation(val info: DruidRelationInfo)(
                        @transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  override val needConversion: Boolean = false;

  override def schema: StructType = {
    var timeField: ArrayBuffer[StructField] = ArrayBuffer()
    val dimensionFields: ArrayBuffer[StructField] = ArrayBuffer()
    val metricFields: ArrayBuffer[StructField] = ArrayBuffer()
    info.druidColumns.map {
      case (columnName: String, relationColumn: DruidRelationColumn) =>
        val (sparkType, isDimension) = relationColumn.druidColumn match {
          case Some(dc) =>
            (DruidDataType.sparkDataType(dc.dataType), dc.isDimension())
          case None => // Some hll metric's origin column may not be indexed
            if (relationColumn.hllMetric.isEmpty &&
              relationColumn.sketchMetric.isEmpty) {
              throw new DruidDataSourceException(s"Illegal column $relationColumn")
            }
            (StringType, true)
        }
        if (columnName == DruidDataSource.INNER_TIME_COLUMN_NAME) {
          // Here specifies time dimension's spark data type as TimestampType.
          timeField += StructField(info.timeDimensionCol, TimestampType)
        } else if (isDimension) {
          dimensionFields += StructField(columnName, sparkType)
        } else {
          metricFields += StructField(columnName, sparkType)
        }
    }
    StructType(timeField ++ dimensionFields ++ metricFields)
  }

  def buildInternalScan: RDD[InternalRow] = null

  override def buildScan(): RDD[Row] = {
    buildInternalScan.asInstanceOf[RDD[Row]]
  }

  override def toString(): String = {
    info.toString
  }
}
