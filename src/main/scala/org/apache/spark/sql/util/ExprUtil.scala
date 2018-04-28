package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object ExprUtil {

  def escapeLikeRegex(v: String): String = {
    org.apache.spark.sql.catalyst.util.StringUtils.escapeLikeRegex(v)
  }

  /**
   * Simplify Cast expression by removing inner most cast if redundant.
   * @param oe
   * @param odt
   * @return
   */
  def simplifyCast(oe: Expression, odt: DataType) = oe match {
    case Cast(ie, idt) if odt.isInstanceOf[NumericType] &&
      (idt.isInstanceOf[DoubleType] || idt.isInstanceOf[FloatType] ||
        idt.isInstanceOf[DecimalType]) => Cast(ie, odt)
    case _ => oe
  }
}
