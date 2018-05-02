package org.rzlabs.druid.jscodegen

import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.rzlabs.druid.{DruidQueryBuilder, JavascriptAggregationSpec}

case class JSAggrGenerator(dqb: DruidQueryBuilder, aggrFunc: AggregateFunction,
                           timeZone: String) extends MyLogging {

  import JSAggrGenerator._

  private[this] def aggrJsFuncSkeleton(argA: String, argB: String, code: String) =
    s"""
       |if (($argA == null || isNaN($argA)) && ($argB == null || isNaN($argB))) {
       |  return null;
       |} else if ($argA == null || isNaN($argA)) {
       |  return $argB;
       |} else if ($argB == null || isNaN($argB)) {
       |  return $argA;
       |} else {
       |  return $code;
       |}
     """.stripMargin

  private[this] def getAggr(arg: String): Option[String] = aggrFunc match {
    case Sum(e) => Some(aggrJsFuncSkeleton("current", arg, s"(current + ($arg))"))
    case Min(e) => Some(aggrJsFuncSkeleton("current", arg, s"Math.min(current, $arg)"))
    case Max(e) => Some(aggrJsFuncSkeleton("current", arg, s"Math.max(current, $arg)"))
    case Count(e) => Some(s"return (current + 1);")
    case _ => None
  }

  private[this] def getCombine(partialA: String, partialB: String): Option[String] = aggrFunc match {
    case Sum(e) => Some(aggrJsFuncSkeleton(partialA, partialB, s"($partialA + $partialB)"))
    case Min(e) => Some(aggrJsFuncSkeleton(partialA, partialB, s"Math.min($partialA, $partialB)"))
    case Max(e) => Some(aggrJsFuncSkeleton(partialA, partialB, s"Math.max($partialA, $partialB)"))
    case Count(e) => Some(s"return ($partialA + $partialB);")
    case _ => None
  }

  private[this] def getReset: Option[String] = aggrFunc match {
    case Sum(e) => Some("return 0;")
    case Min(e) => Some("return Number.POSITIVE_INFINITY;")
    case Max(e) => Some("return Number.NEGATIVE_INFINITY;")
    case Count(e) => Some("return 0;")
    case _ => None
  }

  /**
   * Druid aggregator type could only be LONG, FLOAT or DOUBLE.
   */
  private[this] val jsAggrType: Option[DataType] = aggrFunc match {
    case Count(a :: Nil) => Some(LongType)
    case _ =>
      aggrFunc.dataType match {
        case ShortType | IntegerType | LongType | FloatType | DoubleType => Some(DoubleType)
        case TimestampType => Some(LongType)  // What AggregateFunction's datatype is TimestampType???
        case _ => None
      }
  }

  val druidType: Option[DataType] = aggrFunc match {
    case Count(_) => Some(LongType)
    case _ =>
      aggrFunc.dataType match {
        case ShortType | IntegerType | LongType | FloatType | DoubleType => Some(DoubleType)
        case TimestampType => Some(TimestampType)  // What AggregateFunction's datatype is TimestampType???
        case _ => None
      }
  }

  private[this] val jscodegen: Option[(JSCodeGenerator, String)] =
    for (c <- aggrFunc.children.headOption
         if (aggrFunc.children.size == 1 && !aggrFunc.isInstanceOf[Average]);
         (ce, tf) <- simplifyExpr(dqb, c, timeZone);
         retType <- jsAggrType) yield
      (JSCodeGenerator(dqb, ce, true, true, timeZone, retType), tf)

  val fnAggregate: Option[String] =
    for (codegen <- jscodegen; fne <- codegen._1.fnElements; ret <- getAggr(fne._2)) yield
      s"""function(${("current" :: codegen._1.fnParams).mkString(", ")}) { ${fne._1} $ret }"""

  val fnCombine: Option[String] =
    for (ret <- getCombine("partialA", "partialB")) yield
      s"function(partialA, partialB) { $ret }"

  val fnReset: Option[String] =
    for (ret <- getReset) yield s"function() { $ret }"

  val aggrFnName: Option[String] = aggrFunc match {
    case Sum(_) => Some("SUM")
    case Min(_) => Some("MIN")
    case Max(_) => Some("MAX")
    case Count(_) => Some("COUNT")
    case _ => None
  }

  val fnParams: Option[List[String]] = for (codegen <- jscodegen) yield codegen._1.fnParams

  val valTransFormFn = if (jscodegen.nonEmpty) jscodegen.get._2 else null
}

object JSAggrGenerator {

  def jsAvgCandidate(dqb: DruidQueryBuilder, af: AggregateFunction) = {
    af match {
      case Average(_) if (af.children.size == 1 &&
        !af.children.head.isInstanceOf[LeafExpression]) => true
      case _ => false
    }
  }

  def simplifyExpr(dqb: DruidQueryBuilder, e: Expression, timeZone: String):
  Option[(Expression, String)] = {
    e match {
      case Cast(a @ AttributeReference(nm, _, _, _), TimestampType)
        if (dqb.druidColumn(nm).get.isTimeDimension) =>
        Some((Cast(a, LongType), "toTSWithTZAdj"))
      case _ => Some(e, null)
    }
  }

  def jsAggr(dqb: DruidQueryBuilder, aggrExpr: Expression, af: AggregateFunction,
             tz: String): Option[(DruidQueryBuilder, String)] = {
    val jsAggrGen = JSAggrGenerator(dqb, af, tz)
    for (fnAggr <- jsAggrGen.fnAggregate; fnCbn <- jsAggrGen.fnCombine;
         fnRst <- jsAggrGen.fnReset; fnName <- jsAggrGen.aggrFnName;
         fnAlias <- Some(dqb.nextAlias); fnParams <- jsAggrGen.fnParams;
         druidDataType <- jsAggrGen.druidType) yield {
      (dqb.aggregationSpec(
        new JavascriptAggregationSpec(fnAlias, fnParams, fnAggr, fnCbn, fnRst)).
        outputAttribute(fnAlias, aggrExpr, aggrExpr.dataType, druidDataType,
          jsAggrGen.valTransFormFn), fnAlias)
    }
  }
}
