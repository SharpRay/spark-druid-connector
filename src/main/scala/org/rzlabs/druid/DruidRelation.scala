package org.rzlabs.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.fasterxml.jackson.databind.ObjectMapper._
import org.joda.time.Interval
import org.rzlabs.druid.metadata.{DruidRelationColumn, DruidRelationInfo}

import scala.collection.mutable.ArrayBuffer

case class DruidAttribute(exprId: ExprId, name: String, dataType: DataType, tf: String = null)

/**
 *
 * @param qrySpec
 * @param useSmile
 * @param queryHistoricalServer currently this is always false.
 * @param numSegmentsPerQuery currently this is always -1.
 * @param intervals
 * @param outputAttrSpec Attributes to be output from the RawDataSourceScanExec. Each output attribute is
 *                       based on an Attribute in the original logical plan. The association is based on
 *                       the exprId of `NamedExpression`.
 */
case class DruidQuery(qrySpec: QuerySpec,
                      useSmile: Boolean,
                      queryHistoricalServer: Boolean,
                      numSegmentsPerQuery: Int,
                      intervals: List[Interval],
                      outputAttrSpec: Option[List[DruidAttribute]]
                     ) {

  def this(qrySpec: QuerySpec, useSmile: Boolean = true,
           queryHistoricalServer: Boolean = false,
           numSegmentPerQuery: Int = -1) = {
    this(qrySpec, useSmile, queryHistoricalServer, numSegmentPerQuery,
      qrySpec.intervalList.map(new Interval(_)), None)
  }

  private def schemaFromQuerySpec(drInfo: DruidRelationInfo): StructType = {
    qrySpec.schemaFromQuerySepc(drInfo)
  }

  private lazy val schemaFromOutputSpec: StructType = {
    StructType(outputAttrSpec.getOrElse(Nil).map {
      case DruidAttribute(_, name, dataType, _) =>
        new StructField(name, dataType)
    })
  }

  def schema(drInfo: DruidRelationInfo): StructType = {
    schemaFromOutputSpec.length match {
      case 0 => schemaFromQuerySpec(drInfo)
      case _ => schemaFromOutputSpec
    }
  }

  private def outputAttrsFromQuerySpec(drInfo: DruidRelationInfo): Seq[Attribute] = {
    schemaFromQuerySpec(drInfo).map {
      case StructField(name, dataType, _, _) => AttributeReference(name, dataType)()
    }
  }

  private lazy val outputAttrsFromOutputSpec: Seq[Attribute] = {
    outputAttrSpec.getOrElse(Nil).map {
      case DruidAttribute(exprId, name, dataType, _) =>
        AttributeReference(name, dataType)(exprId)
    }
  }

  def outputAttrs(drInfo: DruidRelationInfo): Seq[Attribute] = {
    outputAttrsFromOutputSpec.size match {
      case 0 => outputAttrsFromQuerySpec(drInfo)
      case _ => outputAttrsFromOutputSpec
    }
  }

  def getValTFMap(): Map[String, String] = {
    outputAttrSpec.getOrElse(Nil).map {
      case DruidAttribute(_, name, _, tf) =>
        name -> tf
    }.toMap
  }
}

case class DruidRelation(val info: DruidRelationInfo, val druidQuery: Option[DruidQuery])(
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

  def buildInternalScan: RDD[InternalRow] = {
    druidQuery.map(new DruidRDD(sqlContext, info, _)).getOrElse(null)
  }

  override def buildScan(): RDD[Row] = {
    buildInternalScan.asInstanceOf[RDD[Row]]
  }

  override def toString(): String = {
    druidQuery.map { dq =>
      s"DruidQuery: ${jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dq)}"
    }.getOrElse {
      info.toString
    }
  }
}
