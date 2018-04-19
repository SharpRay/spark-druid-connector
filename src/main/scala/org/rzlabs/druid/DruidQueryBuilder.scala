package org.rzlabs.druid

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DataType
import org.rzlabs.druid.metadata.{DruidRelationColumn, DruidRelationInfo}

import scala.collection.mutable.{Map => MMap}

case class DruidQueryBuilder(druidRelationInfo: DruidRelationInfo,
                             queryIntervals: QueryIntervals,
                             referencedDruidColumns: MMap[String, DruidRelationColumn] = MMap(),
                             dimensions: List[DimensionSpec] = Nil,
                             limitSpec: Option[LimitSpec] = None,
                             havingSpec: Option[HavingSpec] = None,
                             granularitySpec: Either[String, GranularitySpec] = Left("all"),
                             filterSpec: Option[FilterSpec] = None,
                             aggregations: List[AggregationSpec] = Nil,
                             postAggregations: Option[List[PostAggregationSpec]] = None,
                             projectionAliasMap: Map[String, String] = Map(),
                             outputAttributeMap: Map[String, (Expression, DataType, DataType, String)] = Map(),
                            // avg expressions to perform in the Project operator
                            // on top of Druid PhysicalScan.
                             avgExpressions: Map[Expression, (String, String)] = Map(),
                             aggregateOper: Option[Aggregate] = None,
                             curId: AtomicLong = new AtomicLong(-1),
                             origProjectList: Option[Seq[NamedExpression]] = None,
                             origFilter: Option[Expression] = None
                            ) {

}
