package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand}
import org.rzlabs.druid._

trait AggregateTransform {
  self: DruidPlanner =>

  private def setGroupingInfo(dqb: DruidQueryBuilder,
                              timeElemExtractor: SparkNativeTimeElementExtractor,
                              grpExpr: Expression
                             ): Option[DruidQueryBuilder] = {

    grpExpr match {
      case AttributeReference(nm, dataType, _, _) if dqb.isNonTimeDimension(nm) =>
        val dc = dqb.druidColumn(nm).get
        if (dc.isDimension()) {
          Some(dqb.dimensionSpec(new DefaultDimensionSpec(dc.name, nm)).outputAttribute(nm,
            grpExpr, dataType, DruidDataType.sparkDataType(dc.dataType)))
        } else if (!dc.hasDirectDruidColumn) {
          throw new DruidDataSourceException(s"Column '${dc.name}' is not indexed into datasource.")
        } else {
          throw new DruidDataSourceException(s"Metric '${dc.name}' cannot be used as a dimension.")
        }
      case timeElemExtractor(dtGrp) =>
        val timeFmtExtractFunc: ExtractionFunctionSpec = {
          if (dtGrp.inputFormat.isDefined) {
            new TimeParsingExtractionFunctionSpec(dtGrp.inputFormat.get, dtGrp.formatToApply)
          } else {
            new TimeFormatExtractionFunctionSpec(dtGrp.formatToApply, dtGrp.timeZone.getOrElse(null))
          }
        }
        Some(dqb.dimensionSpec(
          new ExtractionDimensionSpec(dtGrp.druidColumn.name, timeFmtExtractFunc, dtGrp.outputName))
            .outputAttribute(dtGrp.outputName, grpExpr, grpExpr.dataType,
              DruidDataType.sparkDataType(dtGrp.druidColumn.dataType)))
      case _ =>


    }
  }

  private def transformGrouping(dqb: DruidQueryBuilder,
                                   aggOp: Aggregate,
                                   grpExprs: Seq[Expression],
                                   aggrExprs: Seq[NamedExpression]
                                  ): Option[DruidQueryBuilder] = {

    val timeElemExtractor = new SparkNativeTimeElementExtractor(dqb)

    grpExprs.foldLeft(Some(dqb)) {
      (dqb, e) =>

    }
  }

  val aggregateTransform: DruidTransform = {

    case (dqb, Aggregate(_, _, Aggregate(_, _, Expand(_, _, _)))) =>
      // There are more than 1 distinct aggregate expressions.
      // Because Druid cannot handle accurate distinct operation,
      // so we do not push aggregation down to Druid.
      Nil
    case (dqb, agg @ Aggregate(grpExprs, aggrExprs, child)) =>
      // There is 1 distinct aggregate expressions.
      // Because Druid cannot handle accurate distinct operation,
      // so we do not push aggregation down to Druid.
      if (aggrExprs.exists {
        case ne: NamedExpression => ne.find {
          case ae: AggregateExpression if ae.isDistinct => true
          case _ => false
        }.isDefined
      }) Nil {
        // There is no distinct aggregate expressions.
        // Returns Nil if plan returns Nil.
        plan(dqb, child).flatMap { dqb =>
          transformGrouping(dqb, agg, grpExprs, aggrExprs)
        }
      }
  }
}
