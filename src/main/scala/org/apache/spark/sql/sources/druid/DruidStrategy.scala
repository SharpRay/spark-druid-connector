package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{DataSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Divide, Expression, NamedExpression}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ExprUtil
import org.rzlabs.druid._

private[sql] class DruidStrategy(planner: DruidPlanner) extends Strategy
  with MyLogging {

  override def apply(lp: LogicalPlan): Seq[SparkPlan] = {

    val plan: Seq[SparkPlan] = for (dqb <- planner.plan(null, lp)) yield {
      if (dqb.aggregateOper.isDefined) {
        aggregatePlan(dqb)
      } else {
        selectPlan(dqb, lp)
      }
    }

    plan.filter(_ != null).toList
  }

  private def selectPlan(dqb: DruidQueryBuilder, lp: LogicalPlan): SparkPlan = {
    lp match {
      // Just in the case that Project operator as current logical plan
      // the query spec will be generated.
      case Project(projectList, _) => selectPlan(dqb, projectList)
      case _ =>
    }
  }

  private def selectPlan(dqb: DruidQueryBuilder,
                         projectList: Seq[NamedExpression]): SparkPlan = {

    val attrRefs: Seq[Attribute] = for (na <- projectList;
                        attrRef <- na.references) yield attrRef

    // remove the non-indexed dimension column.
    val odqb = attrRefs.foldLeft[Option[DruidQueryBuilder]](Some(dqb)) {
      (ldqb, attr) => ldqb.flatMap { dqb =>
        dqb.druidColumn(attr.name)
      }
    }

    null
  }

  private def aggregatePlan(dqb: DruidQueryBuilder): SparkPlan = {
    val druidSchema = new DruidSchema(dqb)
    val postAgg = new PostAggregate(druidSchema)

    val queryIntervals = dqb.queryIntervals.get

    val qrySpec: QuerySpec = new GroupByQuerySpec(
      dqb.druidRelationInfo.druidDataSource.name,
      dqb.dimensions,
      dqb.limitSpec,
      dqb.havingSpec,
      dqb.granularitySpec,
      dqb.filterSpec,
      dqb.aggregations,
      dqb.postAggregations,
      queryIntervals.map(_.toString),
      Some(QuerySpecContext(s"query-${System.nanoTime()}")))

    // TODO: any necessary transformations.

    qrySpec.context.foreach { ctx =>
      if (dqb.druidRelationInfo.options.useV2GroupByEngine) {
        ctx.groupByStrategy = Some("v2")
      }
    }

    // TODO: handle the cost of post aggregation at historical.
    val queryHistorical = false
    val numSegsPerQuery = -1

    val druidQuery = DruidQuery(qrySpec,
      dqb.druidRelationInfo.options.useSmile,
      queryHistorical,
      numSegsPerQuery,
      queryIntervals,
      Some(druidSchema.druidAttributes))

    def postDruidStep(plan: SparkPlan): SparkPlan = {
      // TODO: always return arg currently
      plan
    }

    def buildProjectList(dqb: DruidQueryBuilder, druidSchema: DruidSchema): Seq[NamedExpression] = {
      buildProjectionList(dqb.aggregateOper.get.aggregateExpressions, druidSchema)
    }

    buildPlan(dqb, druidSchema, druidQuery, planner, postDruidStep _, buildProjectList _)

  }

  private def buildPlan(dqb: DruidQueryBuilder,
                        druidSchema: DruidSchema,
                        druidQuery: DruidQuery,
                        planner: DruidPlanner,
                        postDruidStep: SparkPlan => SparkPlan,
                        buildProjectList: (DruidQueryBuilder, DruidSchema) => Seq[NamedExpression]
                       ): SparkPlan = {

    val druidRelation = DruidRelation(dqb.druidRelationInfo, Some(druidQuery))(planner.sqlContext)

    val druidSparkPlan = postDruidStep(
      DataSourceScanExec.create(druidSchema.schema,
      druidRelation.buildInternalScan, druidRelation)
    )

    if (druidSparkPlan != null) {
      val projections = buildProjectList(dqb, druidSchema)
      ProjectExec(projections, druidSparkPlan)
    } else null
  }

  private def buildProjectionList(origExpressions: Seq[NamedExpression],
                                  druidSchema: DruidSchema): Seq[NamedExpression] = {
    val druidPushDownExprMap: Map[Expression, DruidAttribute] =
      druidSchema.pushedDownExprToDruidAttr
    val avgExpressions = druidSchema.avgExpressions

    origExpressions.map { ne => ExprUtil.transformReplace(ne, {
      case e: Expression if avgExpressions.contains(e) =>
        val (sumAlias, cntAlias) = avgExpressions(e)
        val sumAttr: DruidAttribute = druidSchema.druidAttrMap(sumAlias)
        val cntAttr: DruidAttribute = druidSchema.druidAttrMap(cntAlias)
        Cast(Divide(
          Cast(AttributeReference(sumAttr.name, sumAttr.dataType)(sumAttr.exprId), DoubleType),
          Cast(AttributeReference(cntAttr.name, cntAttr.dataType)(cntAttr.exprId), DoubleType)
        ), e.dataType)
      case ae: AttributeReference if druidPushDownExprMap.contains(ae) &&
        druidPushDownExprMap(ae).dataType != ae.dataType =>
        val da = druidPushDownExprMap(ae)
        Alias(Cast(AttributeReference(da.name, da.dataType)(da.exprId), ae.dataType), da.name)(da.exprId)
      case ae: AttributeReference if druidPushDownExprMap.contains(ae) &&
        druidPushDownExprMap(ae).name != ae.name =>
        val da = druidPushDownExprMap(ae)
        Alias(AttributeReference(da.name, da.dataType)(da.exprId), da.name)(da.exprId)
      case ae: AttributeReference if druidPushDownExprMap.contains(ae) => ae
      case e: Expression if druidPushDownExprMap.contains(e) &&
        druidPushDownExprMap(e).dataType != e.dataType =>
        val da = druidPushDownExprMap(e)
        Cast(AttributeReference(da.name, da.dataType)(da.exprId), e.dataType)
      case e: Expression if druidPushDownExprMap.contains(e) =>
        val da = druidPushDownExprMap(e)
        AttributeReference(da.name, da.dataType)(da.exprId)
    }).asInstanceOf[NamedExpression]}
  }

}
