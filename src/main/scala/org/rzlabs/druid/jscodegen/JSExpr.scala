package org.rzlabs.druid.jscodegen

import org.apache.spark.sql.types.DataType

/**
 *
 * @param fnVar
 * @param linesSoFar
 * @param curLine
 * @param fnDT
 * @param timeDim
 */
private[jscodegen] case class JSExpr(val fnVar: Option[String], val linesSoFar: String,
                                     val curLine: String, val fnDT: DataType,
                                     val timeDim: Boolean = false) {

  private[jscodegen] def this(curLine: String, fnDT: DataType, timeDim: Boolean) = {
    this(None, "", curLine, fnDT, timeDim)
  }

  private[jscodegen] def getRef: String = {
    if (fnVar.isDefined) fnVar.get else curLine
  }
}
