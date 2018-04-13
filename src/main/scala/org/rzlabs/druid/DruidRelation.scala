package org.rzlabs.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
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
        val sparkType = DruidDataType.sparkDataType(relationColumn.druidColumn.dataType)
        if (columnName == DruidDataSource.INNER_TIME_COLUMN_NAME) {
          timeField += StructField(info.timeDimensionCol, sparkType)
        } else if (relationColumn.druidColumn.isDimension()) {
          dimensionFields += StructField(columnName, sparkType)
        } else if (!relationColumn.druidColumn.isDimension()) {
          metricFields += StructField(columnName, sparkType)
        } else {
          throw new DruidDataSourceException("Impossible to get here!")
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
