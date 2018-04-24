package org.rzlabs.druid

import org.apache.spark.sql.types._

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

