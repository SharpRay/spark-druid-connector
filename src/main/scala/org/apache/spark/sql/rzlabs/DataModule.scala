package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.sources.druid.{DruidPlanner, DruidStrategy}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.rzlabs.druid.metadata.DruidOptions

trait DataModule {

  def physicalRules(druidOptions: DruidOptions): Seq[Strategy] = Nil

}

object DruidBaseModule extends DataModule {

  override def physicalRules(druidOptions: DruidOptions): Seq[Strategy] = {
    val druidPlanner = DruidPlanner(druidOptions)
    Seq(new DruidStrategy(druidPlanner))
  }
}
