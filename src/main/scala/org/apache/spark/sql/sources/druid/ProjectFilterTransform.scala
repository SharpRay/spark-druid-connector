package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ExprUtil
import org.rzlabs.druid._
import org.rzlabs.druid.jscodegen.JSCodeGenerator
import org.rzlabs.druid.metadata.DruidRelationColumn

trait ProjectFilterTransform {
  self: DruidPlanner =>

  def addUnpushedAttributes(dqb: DruidQueryBuilder, e: Expression,
                            isProjection: Boolean): Option[DruidQueryBuilder] = {
    if (isProjection) {
      Some(dqb.copy(hasUnpushedProjections = true))
    } else {
      Some(dqb.copy(hasUnpushedFilters = true))
    }
  }

  def projectExpression(dqb: DruidQueryBuilder, projectExpr: Expression,
                        joinAttrs: Set[String] = Set(), ignoreProjectList: Boolean = false
                       ): Option[DruidQueryBuilder] = projectExpr match {

    case _ if ignoreProjectList => Some(dqb)
    case AttributeReference(nm, _, _, _) if dqb.druidColumn(nm).isDefined => Some(dqb)
    case AttributeReference(nm, _, _, _) if joinAttrs.contains(nm) => Some(dqb)
    case Alias(ar @ AttributeReference(nm1, _, _, _), nm) => {
      for (dqbc <- projectExpression(dqb, ar, joinAttrs, ignoreProjectList)) yield
        dqbc.addAlias(nm, nm1)
    }
    case Alias(c @ Cast(ar @ AttributeReference(nm1, _, _, _), _, _), nm) => {
      for (dqbc <- projectExpression(dqb, ar, joinAttrs, ignoreProjectList)) yield
        dqb.addAlias(nm, nm1)
    }
    case _ => addUnpushedAttributes(dqb, projectExpr, true)
  }

  def translateProjectFilter(dqb: Option[DruidQueryBuilder], projectList: Seq[NamedExpression],
                             filters: Seq[Expression], ignoreProjectList: Boolean = false,
                             joinAttrs: Set[String] = Set()): Seq[DruidQueryBuilder] = {
    val dqb1 = if (ignoreProjectList) dqb else {
      projectList.foldLeft(dqb) {
        (ldqb, e) => ldqb.flatMap(projectExpression(_, e, joinAttrs, false))
      }
    }

    if (dqb1.isDefined) { // dqb will never be None
      // A predicate on the time dimension will be rewrites to Interval constraint.
      val ice = new SparkIntervalConditionExtractor(dqb1.get)
      // For each filter generates a new DruidQueryBuilder.
      var odqb = filters.foldLeft(dqb1) { (lodqb, filter) =>
        lodqb.flatMap { ldqb =>
          intervalFilterExpression(ldqb, ice, filter).orElse {
            dimensionFilterExpression(ldqb, filter).map { spec =>
              ldqb.filterSpecification(spec)
            }
          }
        }
      }
      odqb = odqb.map { dqb2 =>
        dqb2.copy(origProjectList = dqb2.origProjectList.map(_ ++ projectList).orElse(Some(projectList))).
          copy(origFilter = dqb2.origFilter.flatMap(f =>
            ExprUtil.and(filters :+ f)).orElse(ExprUtil.and(filters)))
      }
      odqb.map(Seq(_)).getOrElse(Seq())
    } else Seq()
  }

  def intervalFilterExpression(dqb: DruidQueryBuilder, ice: SparkIntervalConditionExtractor,
                               filter: Expression): Option[DruidQueryBuilder] = filter match {
    case ice(ic) => dqb.queryInterval(ic)
    case _ => None
  }

  def dimensionFilterExpression(dqb: DruidQueryBuilder, filter: Expression): Option[FilterSpec] = {

    val timeExtractor = new SparkNativeTimeElementExtractor()(dqb)

    (dqb, filter) match {
      case ValidDruidNativeComparison(filterSpec) => Some(filterSpec)
      case (dqb, filter) => filter match {
        case Or(e1, e2) =>
          Utils.sequence(
            List(dimensionFilterExpression(dqb, e1),
              dimensionFilterExpression(dqb, e2))).map { specs =>
            LogicalExpressionFilterSpec("or", specs)
          }
        case And(e1, e2) =>
          Utils.sequence(
            List(dimensionFilterExpression(dqb, e1),
              dimensionFilterExpression(dqb, e2))).map { specs =>
            LogicalExpressionFilterSpec("and", specs)
          }
        case In(AttributeReference(nm, _, _, _), vl: Seq[Expression]) =>
          for (dc <- dqb.druidColumn(nm) if dc.isDimension() &&
            vl.forall(_.isInstanceOf[Literal])) yield
            new InFilterSpec(dc.name, vl.map(_.asInstanceOf[Literal].value.toString).toList)
        case InSet(AttributeReference(nm, _, _, _), vl: Set[Any]) =>
          for (dc <- dqb.druidColumn(nm) if dc.isDimension()) yield
            new InFilterSpec(dc.name, vl.map(_.toString).toList)
        case IsNotNull(AttributeReference(nm, _, _, _)) =>
          for (dc <- dqb.druidColumn(nm) if dc.isDimension()) yield
            new NotFilterSpec(new SelectorFilterSpec(dc.name, ""))
        case IsNull(AttributeReference(nm, _, _, _)) =>
          for (dc <- dqb.druidColumn(nm) if dc.isDimension()) yield
            new SelectorFilterSpec(nm, "")
        case Not(e) =>
          for (spec <- dimensionFilterExpression(dqb, e)) yield
            new NotFilterSpec(spec)
        // TODO: What is NULL SCAN ???
        case Literal(null, _) => Some(new SelectorFilterSpec("__time", ""))
        case _ => {
          val jscodegen = JSCodeGenerator(dqb, filter, false, false,
            dqb.druidRelationInfo.options.timeZoneId, BooleanType)
          for (fn <- jscodegen.fnCode) yield {
            new JavascriptFilterSpec(jscodegen.fnParams.last, fn)
          }
        }
      }
    }
  }

  private def boundFilter(dqb: DruidQueryBuilder, e: Expression, dc: DruidRelationColumn,
                     value: Any, sparkDt: DataType, op: String): FilterSpec = {

    val druidDs = dqb.druidRelationInfo.druidDataSource
    val ordering = sparkDt match {
      case ShortType | IntegerType | LongType |
           FloatType | DoubleType | DecimalType() => "numeric"
      case _ => "lexicographic"
    }
    if (druidDs.supportsBoundFilter) { // druid 0.9.0+ support bound filter.
      op match {
        case "<" => new BoundFilterSpec(dc.name, null, value.toString, false, true, ordering)
        case "<=" => new BoundFilterSpec(dc.name, null, value.toString, false, false, ordering)
        case ">" => new BoundFilterSpec(dc.name, value.toString, null, true, false, ordering)
        case ">=" => new BoundFilterSpec(dc.name, value.toString, null, false, false, ordering)
        case _ => null
      }
    } else {
      val jscodegen = new JSCodeGenerator(dqb, e, false, false,
        dqb.druidRelationInfo.options.timeZoneId)
      val v = if (ordering == "numeric") value.toString else s""""${value.toString}""""
      val ospec: Option[FilterSpec] = for (fn <- jscodegen.fnElements;
           body <- Some(fn._1); returnVal <- Some(fn._2)) yield {
        val jsFn =
          s"""function(${dc.name}) {
             |  ${body};
             |  if (($returnVal) $op $v) {
             |    return true;
             |  } else {
             |    return false;
             |  }
             |}""".stripMargin
        new JavascriptFilterSpec(dc.name, jsFn)
      }
      ospec.getOrElse(null)
    }
  }

  object ValidDruidNativeComparison {

    def unapply(t: (DruidQueryBuilder, Expression)): Option[FilterSpec] = {
      import SparkNativeTimeElementExtractor._
      val dqb = t._1
      val filter = t._2
      val timeExtractor = new SparkNativeTimeElementExtractor()(dqb)
      filter match {
        case EqualTo(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield new SelectorFilterSpec(dc.name, value.toString)
        case EqualTo(Literal(value, _), AttributeReference(nm, dt, _, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield new SelectorFilterSpec(dc.name, value.toString)
        case EqualTo(AttributeReference(nm1, _, _, _), AttributeReference(nm2, _, _, _)) =>
          for (dc1 <- dqb.druidColumn(nm1) if dc1.isDimension();
               dc2 <- dqb.druidColumn(nm2) if dc2.isDimension()) yield
            new ColumnComparisonFilterSpec(List(dc1.name, dc2.name))
        case LessThan(ar @ AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, "<")
        case LessThan(Literal(value, _), ar @ AttributeReference(nm, dt, _, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, ">")
        case LessThanOrEqual(ar @ AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, "<=")
        case LessThanOrEqual(Literal(value, _), ar @ AttributeReference(nm, dt, _, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, ">=")
        case GreaterThan(ar @ AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, ">")
        case GreaterThan(Literal(value, _), ar @ AttributeReference(nm, dt, _, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, "<")
        case GreaterThanOrEqual(ar @ AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, ">=")
        case GreaterThanOrEqual(Literal(value, _), ar @ AttributeReference(nm, dt, _, _)) =>
          for (dc <- dqb.druidColumn(nm)
               if dc.isDimension() && DruidDataType.sparkDataType(dc.dataType) == dt)
            yield boundFilter(dqb, ar, dc, value, dt, "<=")
        case _ => None
      }
    }
  }

  val druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l @ LogicalRelation(d @ DruidRelation(info, _), _, _, _))) =>
      // This is the initial DruidQueryBuilder which all transformations
      // are based on.
      val dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))
      val (newFilters, dqb1) = ExprUtil.simplifyConjPred(dqb.get, filters)
      translateProjectFilter(Some(dqb1), projectList, newFilters)
    case _ => Nil
  }
}
