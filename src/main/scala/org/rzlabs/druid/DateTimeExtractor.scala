package org.rzlabs.druid

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.rzlabs.druid.metadata.DruidRelationColumn

/**
 * @param outputName The output name of the column.
 * @param druidColumn The druid column.
 * @param formatToApply The output date format.
 * @param timeZone The output date time zone.
 * @param pushedExpression This controls the expression evaluation that happens
 *                         on return from Druid. So for expression like
 *                         {{{to_date(cast(dateCol as DateType))}}} is evaluated
 *                         on the resultset of Druid. This is required because
 *                         Dates are Ints and Timestamps are Longs in Spark, whereas
 *                         the value coming out of Druid is an ISO DateTime String.
 * @param inputFormat Format to use to parse input value.
 */
case class DateTimeGroupingElem(outputName: String,
                                druidColumn: DruidRelationColumn,
                                formatToApply: String,
                                timeZone: Option[String],
                                pushedExpression: Expression,
                                inputFormat: Option[String] = None)

object DruidColumnExtractor {

  def unapply(e: Expression)(
    implicit dqb: DruidQueryBuilder): Option[DruidRelationColumn] = e match {
    case AttributeReference(nm, _, _, _) =>
      val druidColumn = dqb.druidColumn(nm)
      druidColumn.filter(_.isDimension())
    case _ => None
  }
}

class SparkNativeTimeElementExtractor(implicit val dqb: DruidQueryBuilder) {

  self =>

  import SparkNativeTimeElementExtractor._

  def unapply(e: Expression): Option[DateTimeGroupingElem] = e match {
    case DruidColumnExtractor(dc) if e.dataType == DateType =>
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, DATE_FORMAT,
        Some(dqb.druidRelationInfo.options.timeZoneId), e))
    case Cast(c @ DruidColumnExtractor(dc), DateType) =>
      // e.g., "cast(time as date)"
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, DATE_FORMAT,
        Some(dqb.druidRelationInfo.options.timeZoneId), c))
    case Cast(self(dtGrp), DateType) =>
      // e.g., "cast(from_utc_timestamp(time, 'GMT') as date)", include last case
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        DATE_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case DruidColumnExtractor(dc) if e.dataType == StringType =>
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, TIMESTAMP_FORMAT,
        Some(dqb.druidRelationInfo.options.timeZoneId), e))
    case Cast(self(dtGrp), StringType) =>
      // e.g., "cast(time as string)"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        dtGrp.formatToApply, dtGrp.timeZone, e))
    case DruidColumnExtractor(dc) if e.dataType == TimestampType =>
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, TIMESTAMP_FORMAT,
        Some(dqb.druidRelationInfo.options.timeZoneId), e))
    case Cast(c @ DruidColumnExtractor(dc), TimestampType) =>
      // e.g., "cast(time as timestamp)"
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, TIMESTAMP_FORMAT,
        Some(dqb.druidRelationInfo.options.timeZoneId), c))
    case Cast(self(dtGrp), TimestampType) =>
      // e.g., "cast(to_date(time) as timestamp)", include last case
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        TIMESTAMP_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case ToDate(self(dtGrp)) =>
      // e.g., "to_date(time)"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        DATE_FORMAT, dtGrp.timeZone,dtGrp.pushedExpression))
    case Year(self(dtGrp)) =>
      // e.g., "year(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        YEAR_FORMAT, dtGrp.timeZone, e))
    case DayOfMonth(self(dtGrp)) =>
      // e.g., "dayofmonth(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        DAY_OF_MONTH_FORMAT, dtGrp.timeZone, e))
    case DayOfYear(self(dtGrp)) =>
      // e.g., "dayofyear(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        DAY_OF_YEAR_FORMAT, dtGrp.timeZone, e))
    case Month(self(dtGrp)) =>
      // e.g., "month(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        MONTH_FORMAT, dtGrp.timeZone, e))
    case WeekOfYear(self(dtGrp)) =>
      // e.g., "weekofyear(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        WEEKOFYEAR_FORMAT, dtGrp.timeZone, e))
    case Hour(self(dtGrp)) =>
      // e.g., "hour(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        HOUR_FORMAT, dtGrp.timeZone, e))
    case Minute(self(dtGrp)) =>
      // e.g., "minute(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        MINUTE_FORMAT, dtGrp.timeZone, e))
    case Second(self(dtGrp)) =>
      // e.g., "second(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        SECOND_FORMAT, dtGrp.timeZone, e))
    case UnixTimestamp(self(dtGrp), Literal(inFmt, StringType)) =>
      // e.g., "unix_timestamp(cast(time as date), 'YYYY-MM-dd HH:mm:ss')"

      // TODO: UnixTImestamp should parse with JSGenerator
      // This because TimeFormatExtractionFunctionSpec just return
      // string not bigint.
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        TIMESTAMP_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression,
        Some(inFmt.toString)))
    case UnixTimestamp(c @ DruidColumnExtractor(dc), Literal(inFmt, StringType)) =>
      // e.g., "unix_timestamp(time, 'YYYY-MM-dd HH:mm:ss')"

      // TODO: UnixTImestamp should parse with JSGenerator
      // This because TimeFormatExtractionFunctionSpec just return
      // string not bigint.
      Some(DateTimeGroupingElem(dqb.nextAlias, dc,
        TIMESTAMP_FORMAT, None, c,
        Some(inFmt.toString)))
    case FromUnixTime(self(dtGrp), Literal(outFmt, StringType)) =>
      // TODO: Remove this case because the TimeFormatExtractionFunctionSpec
      // cannot represent the bigint input. We should use
      // JavascriptExtractionFunctionSpec out of here.
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        outFmt.toString, dtGrp.timeZone, e))
    case FromUnixTime(c @ DruidColumnExtractor(dc), Literal(outFmt, StringType)) =>
      // TODO: Remove this case because the TimeFormatExtractionFunctionSpec
      // cannot represent the bigint input. We should use
      // JavascriptExtractionFunctionSpec out of here.
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, outFmt.toString,
        Some(dqb.druidRelationInfo.options.timeZoneId), e))
    case FromUTCTimestamp(self(dtGrp), Literal(tz, StringType)) =>
      // e.g., "from_utc_timestamp(cast(time as timestamp), 'GMT')"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        TIMESTAMP_FORMAT, Some(tz.toString), dtGrp.pushedExpression))
    case FromUTCTimestamp(c @ DruidColumnExtractor(dc), Literal(tz, StringType)) =>
      // e.g., "from_utc_timestamp(time, 'GMT')"
      Some(DateTimeGroupingElem(dqb.nextAlias, dc,
        TIMESTAMP_FORMAT, Some(tz.toString), e))
    case ToUTCTimestamp(self(dtGrp), _) =>
      // e.g., "to_utc_timestamp(cast(time as timestamp), 'GMT')"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.druidColumn,
        TIMESTAMP_FORMAT, None, dtGrp.pushedExpression))
    case ToUTCTimestamp(c @ DruidColumnExtractor(dc), _) =>
      // e.g., "to_utc_timestamp(time, 'GMT')"
      Some(DateTimeGroupingElem(dqb.nextAlias, dc,
        TIMESTAMP_FORMAT, None, e))
    case _ => None
  }
}

object SparkNativeTimeElementExtractor {

  val DATE_FORMAT = "YYYY-MM-dd"
  val TIMESTAMP_FORMAT = "YYYY-MM-dd HH:mm:ss"

  val YEAR_FORMAT = "YYYY"
  val MONTH_FORMAT = "MM"
  val WEEKOFYEAR_FORMAT = "ww"
  val DAY_OF_MONTH_FORMAT = "dd"
  val DAY_OF_YEAR_FORMAT = "DD"

  val HOUR_FORMAT = "HH"
  val MINUTE_FORMAT = "mm"
  val SECOND_FORMAT = "ss"
}
