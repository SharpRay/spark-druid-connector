package org.rzlabs.druid.jscodegen

import org.apache.spark.sql.types._

case class JSCast(from: JSExpr, toDT: DataType, ctx: JSCodeGenerator) {

  import JSDateTimeCtx._

  private[jscodegen] val castCode: Option[JSExpr] = toDT match {
    case _ if from.fnDT == toDT => Some(from)
    case BooleanType => castToBooleanType
    case ShortType => castToNumericCode(ShortType)
    case IntegerType => castToNumericCode(IntegerType)
    case LongType => castToNumericCode(LongType)
    case FloatType => castToNumericCode(FloatType)
    case DoubleType => castToNumericCode(DoubleType)
    case StringType => castToStringCode
    case DateType => castToDateCode
    case TimestampType => castToTimestampCode
    case _ => None
  }

  private[this] def castToBooleanType: Option[JSExpr] = from.fnDT match {
    case IntegerType | LongType | FloatType | DoubleType =>
      Some(JSExpr(None, from.linesSoFar, s"Boolean(${from.getRef})", BooleanType))
    case StringType | DateType =>
      // Spark would return null when cast from date to boolean
      // Druid will return null value if the value is null or "".
      Some(JSExpr(None, from.linesSoFar, "null", BooleanType))
    case TimestampType =>
      // Boolean(TimestampType) should always returns true
      // which behaves the same as Spark.
      Some(JSExpr(None, from.linesSoFar, "true", BooleanType))
  }

  private[this] def castToNumericCode(outDt: DataType): Option[JSExpr] = from.fnDT match {
    case BooleanType | StringType =>
      Some(JSExpr(None, from.linesSoFar, s"Number(${from.getRef})", outDt))
    case (FloatType | DoubleType) if ctx.isIntegralNumeric(outDt) =>
      Some(JSExpr(None, from.linesSoFar, s"Math.floor(${from.getRef})", outDt))
    case ShortType | IntegerType | LongType | FloatType | DoubleType =>
      Some(JSExpr(None, from.linesSoFar, from.getRef, outDt))
    case DateType =>
      // Behave the same with Spark. (cast(cast('2018-01-01' as date) as int))
      Some(JSExpr(None, from.linesSoFar, "null", outDt))
    case TimestampType =>
      // Behave the same with Spark. (cast(cast('2018-01-01' as timestamp) as long))
      Some(JSExpr(None, from.linesSoFar, dtToLongCode(from.getRef), outDt))
    case _ => None
  }

  private[this] def castToStringCode: Option[JSExpr] = from.fnDT match {
    case LongType if from.timeDim =>
      // time dimension
      nullSafeCastToString(dtToStrCode(longToISODtCode(from.getRef, ctx.dateTimeCtx)))
    case BooleanType | ShortType | IntegerType | LongType | FloatType
         | DoubleType | DecimalType() => nullSafeCastToString(from.getRef)
    case DateType => nullSafeCastToString(dateToStrCode(from.getRef))
    case TimestampType => nullSafeCastToString(dtToStrCode(from.getRef))
    case _ => None
  }

  private[this] def castToDateCode: Option[JSExpr] = from.fnDT match {
    case StringType =>
      Some(JSExpr(None, from.linesSoFar, stringToDateCode(from.getRef, ctx.dateTimeCtx), DateType))
    case TimestampType =>
      Some(JSExpr(None, from.linesSoFar, dtToDateCode(from.getRef), DateType))
    case LongType if from.timeDim =>
      Some(JSExpr(None, from.linesSoFar, longToDateCode(from.getRef, ctx.dateTimeCtx), DateType))
    case _ => None
  }

  private[this] def castToTimestampCode: Option[JSExpr] = from.fnDT match {
    case StringType =>
      Some(JSExpr(None, from.linesSoFar, stringToISODtCode(from.getRef, ctx.dateTimeCtx, true, ), TimestampType))
    case BooleanType =>
      Some(JSExpr(None, from.linesSoFar, stringToISODtCode(
        s""" (${from.getRef}) == true ? "T00:00:01Z" : "T00:00:00Z"""", ctx.dateTimeCtx), TimestampType))
    case FloatType | DoubleType | DecimalType() =>
      for (lc <- castToNumericCode(LongType)) yield
        JSExpr(None, lc.linesSoFar, longToISODtCode(lc.getRef, ctx.dateTimeCtx), TimestampType)
    case ShortType | IntegerType | LongType =>
      Some(JSExpr(None, from.linesSoFar, longToISODtCode(from.getRef, ctx.dateTimeCtx), TimestampType))
    case DateType =>
      Some(JSExpr(None, from.linesSoFar, localDateToDtCode(from.getRef, ctx.dateTimeCtx), TimestampType))
    case _ => None
  }

  private[this] def nullSafeCastToString(valToCast: String): Option[JSExpr] = {
    if (from.fnVar.isEmpty) {
      val vn = ctx.makeUniqueVarName
      Some(JSExpr(None, from.linesSoFar + s"$vn = $valToCast;",
        s"""($vn != null && !isNaN($vn) ? $vn.toString() : "")""", StringType))
    } else {
      Some(JSExpr(None, from.linesSoFar,
        s"""($valToCast != null && !isNaN($valToCast) ? $valToCast.toString() : "")""",
        StringType))
    }
  }
}
