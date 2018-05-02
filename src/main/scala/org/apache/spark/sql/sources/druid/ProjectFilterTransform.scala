package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.rzlabs.druid.{DruidQueryBuilder, DruidRelation}

class ProjectFilterTransform {
  self: DruidPlanner =>

  def druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l @ LogicalRelation(d @ DruidRelation(info), _, _))) =>
      val dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))

  }
}
