package org.rzlabs.druid.jscodegen

import org.apache.spark.sql.types._

case class JSCast(from: JSExpr, toDT: DataType, ctx: JSCodeGenerator) {

  import JSDateTimeCtx._

  private[jscodegen] val castCode: Option[JSExpr] = toDT match {
    case _ if from.fnDT == NullType =>
      Some(JSExpr(None, from.linesSoFar, from.getRef, StringType))
    case BooleanType =>

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
