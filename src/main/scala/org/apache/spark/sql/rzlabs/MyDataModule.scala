package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.{SparkSession, Strategy}

trait MyDataModule {

  def physicalRules(sparkSession: SparkSession): Seq[Strategy] = Nil

}

object BaseModule extends MyDataModule {

  override def physicalRules(sparkSession: SparkSession): Seq[Strategy] = {

  }
}
