package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.rzlabs.druid.DruidQueryBuilder
import org.rzlabs.druid.client.ConnectionManager
import org.rzlabs.druid.metadata.DruidOptions

class DruidPlanner(val druidOptions: DruidOptions) extends DruidTransforms
  with AggregateTransform with ProjectFilterTransform {

  val transforms: Seq[DruidTransform] = Seq(
    aggregateTransform.debug("aggregate")

  )

  def plan(dqb: Seq[DruidQueryBuilder], plan: LogicalPlan): Seq[DruidQueryBuilder] = {
    transforms.view.flatMap(_(dqb, plan))
  }
}

object DruidPlanner {

  def apply(druidOptions: DruidOptions) = {
    val planner = new DruidPlanner(druidOptions)
    ConnectionManager.init(druidOptions)
    planner
  }
}
