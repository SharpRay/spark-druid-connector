package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType
import org.rzlabs.druid.{DruidAttribute, DruidQueryBuilder}

class DruidSchema(val dqb: DruidQueryBuilder) {

  def avgExpressions: Map[Expression, (String, String)] = dqb.avgExpressions

  lazy val druidAttributes: List[DruidAttribute] = druidAttrMap.values.toList

  lazy val druidAttrMap: Map[String, DruidAttribute] = buildDruidAttr

  lazy val schema: List[Attribute] = druidAttributes.map {
    case DruidAttribute(exprId, name, druidDT, tf) =>
      AttributeReference(name, druidDT)(exprId)
  }

  lazy val pushedDownExprToDruidAttr: Map[Expression, DruidAttribute] =
    buildPushDownDruidAttrsMap

  private def buildPushDownDruidAttrsMap: Map[Expression, DruidAttribute] = {
    dqb.outputAttributeMap.map {
      case (name, (expr, _, _, _)) => expr -> druidAttrMap(name)
    }
  }

  private def buildDruidAttr: Map[String, DruidAttribute] = {

    dqb.outputAttributeMap.map {
      case (name, (expr, _, druidDT, tf)) => {
        val druidExprId = expr match {
          case null => NamedExpression.newExprId
          case ne: NamedExpression => ne.exprId
          case _ => NamedExpression.newExprId
        }
        (name -> DruidAttribute(druidExprId, name, druidDT, tf))
      }
    }
  }
}
