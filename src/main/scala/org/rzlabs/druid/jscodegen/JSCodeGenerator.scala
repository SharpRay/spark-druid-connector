package org.rzlabs.druid.jscodegen

import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.rzlabs.druid.DruidQueryBuilder
import org.rzlabs.druid._

/**
 * Generate Javascript code used in [[JavascriptAggregationSpec]], [[JavascriptExtractionFunctionSpec]],
 * [[JavascriptFilterSpec]] and [[JavascriptPostAggregationSpec]]
 * @param dqb The DruidQueryBuilder whose ''dimensionSpec'' method will be called.
 * @param e The input expression use to generate js code.
 * @param multiInputParamsAllowed If multiply input parameters allowed for the Javascript function.
 *                                NOTE: This will be true if generate [[JavascriptAggregationSpec]].
 * @param metricAllowed If druid metric will be allowed as the input parameter of the Javascript function.
 *                      NOTE: This will be true if generate [[JavascriptAggregationSpec]].
 * @param timeZone The time zone specified.
 * @param retType The return type of the Javascript function, default to StringType.
 */
case class JSCodeGenerator(dqb: DruidQueryBuilder, e: Expression, multiInputParamsAllowed: Boolean,
                           metricAllowed: Boolean, timeZone: String, retType: DataType = StringType
                          ) extends MyLogging {

  private[this] var vid: Int = 0

  private[jscodegen] def makeUniqueVarName: String = {
    vid += 1
    "v" + vid
  }

  private[jscodegen] val dateTimeCtx = new JSDateTimeCtx(timeZone, this)

  private[this] var inParams: Set[String] = Set()

  private[jscodegen] def fnElements: Option[(String, String)] = None

  private[jscodegen] def isIntegralNumeric(dt: DataType) = dt match {
    case ShortType | IntegerType | LongType => true
    case _ => false
  }

  def fnCode: Option[String] = None

  private[this] def genExprCode(oe: Any): Option[JSExpr] = {
    if (oe.isInstanceOf[Expression]) {
      val e: Expression = oe.asInstanceOf[Expression]
      e match {

      }
    } else {
      oe match {
        case _: Short => Some(new JSExpr(s"$oe", ShortType, false))
        case _: Int => Some(new JSExpr(s"$oe", IntegerType, false))
        case _: Long => Some(new JSExpr(s"$oe", LongType, false))
        case _: Float => Some(new JSExpr(s"$oe", FloatType, false))
        case _: Double => Some(new JSExpr(s"$oe", DoubleType, false))
        case _: String => Some(new JSExpr(oe.asInstanceOf[String], StringType, false))
        case _ => None
      }
    }
  }

  private[this] def validInParams(inParam: String): Boolean = {
    inParams = inParams + inParam
    if (!multiInputParamsAllowed && inParams.size > 1) false else true
  }

}
