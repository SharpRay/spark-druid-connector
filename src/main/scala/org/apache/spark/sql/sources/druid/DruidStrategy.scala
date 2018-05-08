package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{DataSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Divide, Expression, NamedExpression}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ExprUtil
import org.rzlabs.druid._

import scala.collection.mutable.{ Map => MMap }

private[sql] class DruidStrategy(planner: DruidPlanner) extends Strategy
  with MyLogging {

  override def apply(lp: LogicalPlan): Seq[SparkPlan] = {

    val plan: Seq[SparkPlan] = for (dqb <- planner.plan(null, lp)) yield {
      if (dqb.aggregateOper.isDefined) {
        aggregatePlan(dqb)
      } else {
        scanPlan(dqb, lp)
      }
    }

    plan.filter(_ != null).toList
  }

  private def scanPlan(dqb: DruidQueryBuilder, lp: LogicalPlan): SparkPlan = {
    lp match {
      // Just in the case that Project operator as current logical plan
      // the query spec will be generated
      case Project(projectList, _) => scanPlan(dqb, projectList)
      case _ => null
    }
  }

  private def scanPlan(dqb: DruidQueryBuilder,
                       projectList: Seq[NamedExpression]): SparkPlan = {

    var dqb1 = dqb

    // Set outputAttrs with projectList
    for (na <- projectList;
         attr <- na.references;
         dc <- dqb.druidColumn(attr.name)) {
      dqb1 = dqb1.outputAttribute(attr.name, attr, attr.dataType,
        DruidDataType.sparkDataType(dc.dataType), null)
    }

    // Set outputAttrs with filters
    for (e <- dqb.origFilter;
         attr <- e.references;
         dc <- dqb.druidColumn(attr.name)) {
      dqb1 = dqb1.outputAttribute(attr.name, attr, attr.dataType,
        DruidDataType.sparkDataType(dc.dataType), null)
    }

    val druidSchema = new DruidSchema(dqb1)

    val intervals = dqb1.queryIntervals.get

    var qrySpec: QuerySpec =
      new ScanQuerySpec(dqb1.druidRelationInfo.druidDataSource.name,
        dqb1.referencedDruidColumns.values.map(_.name).toList,
        dqb1.filterSpec,
        intervals.map(_.toString),
        None,
        None,
        Some(QuerySpecContext(s"query-${System.nanoTime()}"))
      )

    val queryHistorical = false
    val numSegsPerQuery = -1

    val druidQuery = DruidQuery(qrySpec,
      dqb1.druidRelationInfo.options.useSmile,
      false, -1,
      intervals,
      Some(druidSchema.druidAttributes))

    def postDruidStep(plan: SparkPlan): SparkPlan = {
      // TODO: always return arg currently
      plan
    }

    def buildProjectList(dqb: DruidQueryBuilder, druidSchema: DruidSchema): Seq[NamedExpression] = {
      buildProjectionList(projectList, druidSchema)
    }

    buildPlan(dqb1, druidSchema, druidQuery, planner, postDruidStep _, buildProjectList _)
  }

//  private def selectPlan(dqb: DruidQueryBuilder, lp: LogicalPlan): SparkPlan = {
//    lp match {
//      // Just in the case that Project operator as current logical plan
//      // the query spec will be generated.
//      case Project(projectList, _) => selectPlan(dqb, projectList)
//      case _ =>
//    }
//  }

//  private def selectPlan(dqb: DruidQueryBuilder,
//                         projectList: Seq[NamedExpression]): SparkPlan = {
//
//
//    // Replace __time with timestamp because Druid returns 'tiemstamp' field
//    // by default to represent the timestamp string.
//    val referredDruidColumn = dqb.referencedDruidColumns.mapValues { dc =>
//      if (dc.isTimeDimension) {
//        // The dataType of druidColumn of type DruidColumn in
//        // DruidRelationColumn has already been set as StringType.
//        dc.copy(druidColumn =
//          dc.druidColumn.map(d =>
//            d.asInstanceOf[DruidTimeDimension].copy(name = DruidDataSource.TIMESTAMP_KEY_NAME)))
//      } else dc
//    }
//
//    var dqb1 = dqb.copy(referencedDruidColumns = MMap(referredDruidColumn.toSeq: _*))
//
//    // Set outputAttrs with projectList
//    for (na <- projectList;
//         attr <- na.references;
//         dc <- dqb.druidColumn(attr.name)) {
//      dqb1 = dqb1.outputAttribute(attr.name, attr, attr.dataType,
//        DruidDataType.sparkDataType(dc.dataType), null)
//    }
//
//    // Set outputAttrs with filters
//    for (e <- dqb.origFilter;
//         attr <- e.references;
//         dc <- dqb.druidColumn(attr.name)) {
//      dqb1 = dqb1.outputAttribute(attr.name, attr, attr.dataType,
//        DruidDataType.sparkDataType(dc.dataType), null)
//    }
//
//    val druidSchema = new DruidSchema(dqb1)
//
//    var (dims, metrics) = dqb1.referencedDruidColumns.values.partition(_.isDimension())
//
//    // Remove dimension with name 'timestamp' because Druid will return this field.
//    dims = dims.filterNot(_.druidColumn.filter(_ == DruidDataSource.TIMESTAMP_KEY_NAME).nonEmpty)
//
//    /*
//     * If dimensions or metrics are empty, arbitrarily pick 1 dimension and metric.
//     * Otherwise Druid will return all dimensions/metrics.
//     */
//    if (dims.isEmpty) {
//      dims = dqb1.druidRelationInfo.druidColumns.find(_._2.isDimension(true)).map(_._2)
//    }
//    if (metrics.isEmpty) {
//      metrics = dqb1.druidRelationInfo.druidColumns.find(_._2.isMetric).map(_._2)
//    }
//
//    val intervals = dqb1.queryIntervals.get
//
//    null
//  }

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
