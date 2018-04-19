package org.apache.spark.sql.sources.druid

import org.rzlabs.druid.client.ConnectionManager
import org.rzlabs.druid.metadata.DruidOptions

class DruidPlanner(val druidOptions: DruidOptions) {

}

object DruidPlanner {

  def apply(druidOptions: DruidOptions) = {
    val planner = new DruidPlanner(druidOptions)
    ConnectionManager.init(druidOptions)
    planner
  }
}
