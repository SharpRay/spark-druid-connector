package org.rzlabs.druid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.rzlabs.druid.metadata.DruidRelationInfo

class DruidRDD(sqlContext: SQLContext,
               drInfo: DruidRelationInfo,
               val druidQuery: DruidQuery
              ) extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  override def getPartitions: Array[Partition] = Array()

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = null

}


object DruidValTransform {

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

