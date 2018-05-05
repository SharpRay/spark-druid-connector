package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.rzlabs.druid.DruidQueryBuilder
import org.rzlabs.druid.client.ConnectionManager
import org.rzlabs.druid.metadata.DruidOptions

class DruidPlanner(val sqlContext: SQLContext, val druidOptions: DruidOptions) extends DruidTransforms
  with AggregateTransform with ProjectFilterTransform {

  val transforms: Seq[DruidTransform] = Seq(
    aggregateTransform.debug("aggregate"),
    druidRelationTransform.debug("druidRelationTransform")
  )

  def plan(dqb: Seq[DruidQueryBuilder], plan: LogicalPlan): Seq[DruidQueryBuilder] = {
    transforms.view.flatMap(_(dqb, plan))
  }
}

object DruidPlanner {

  def apply(sqlContext: SQLContext, druidOptions: DruidOptions) = {
    val planner = new DruidPlanner(sqlContext, druidOptions)
    ConnectionManager.init(druidOptions)
    planner
  }
}
