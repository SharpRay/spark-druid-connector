package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.sources.druid.{DruidPlanner, DruidStrategy}
import org.apache.spark.sql.{SQLContext, SparkSession, Strategy}
import org.rzlabs.druid.metadata.DruidOptions

trait DataModule {

  def physicalRules(sqlContext: SQLContext, druidOptions: DruidOptions): Seq[Strategy] = Nil

}

object DruidBaseModule extends DataModule {

  override def physicalRules(sqlContext: SQLContext, druidOptions: DruidOptions): Seq[Strategy] = {
    val druidPlanner = DruidPlanner(sqlContext, druidOptions)
    Seq(new DruidStrategy(druidPlanner))
  }
}
