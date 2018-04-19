package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.rzlabs.druid.DruidQueryBuilder

class DruidTransforms extends MyLogging {
  druidPlanner: DruidPlanner => // DruidTransforms can only be inherited by DruidPlanner

  type DruidTransform = Function[(Seq[DruidQueryBuilder], LogicalPlan), Seq[DruidQueryBuilder]]

  case class ORTransform(t1: DruidTransform, t2: DruidTransform) extends DruidTransform {

    def apply(p: (Seq[DruidQueryBuilder], LogicalPlan)): Seq[DruidQueryBuilder] = {

      val r = t1(p._1, p._2)
      if (r.size > 0) {
        r
      } else {
        t2(p._1, p._2)
      }
    }
  }

  case class DebugTransform(transformName: String, t: DruidTransform) extends DruidTransform {

    def apply(p: (Seq[DruidQueryBuilder], LogicalPlan)): Seq[DruidQueryBuilder] = {

      val dqb = p._1
      val lp = p._2
      val rdqb = t(dqb, lp)
      if (druidPlanner.druidOptions.debugTransformations) {
        logInfo(s"$transformName transform invoked:\n" +
          s"Input DruidQueryBuilders: $dqb\n" +
          s"Input LogicalPlan: $lp\n" +
          s"Output DruidQueryBuilders: $rdqb\n")
      }
      rdqb
    }
  }

  case class TransfomHolder(t: DruidTransform) {

    def or(t2: DruidTransform): DruidTransform = ORTransform(t, t2)

    def debug(name: String): DruidTransform = DebugTransform(name, t)
  }

  /**
   * Convert an object's type from DruidTransform to TransformHolder implicitly.
   * So we can call "transform1.or(tramsfomr2)" or "transform1.debug("transformName")".
   * @param t The input DruidTransform object.
   * @return The converted TransformHoder object.
   */
  implicit def transformToHolder(t: DruidTransform) = TransfomHolder(t)
}
