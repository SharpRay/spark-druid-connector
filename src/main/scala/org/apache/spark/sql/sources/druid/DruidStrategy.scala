package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{MyLogging, Strategy}

private[sql] class DruidStrategy(planner: DruidPlanner) extends Strategy
  with MyLogging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    println("hahahahahahaha effective!!!!!!!")
    Nil
  }
}
