package org.rzlabs.druid.jscodegen

import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.rzlabs.druid.DruidQueryBuilder
import org.rzlabs.druid._
import JSDateTimeCtx._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.util.ExprUtil
import org.apache.spark.sql.util.ExprUtil._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Generate Javascript code used in [[JavascriptAggregationSpec]], [[JavascriptExtractionFunctionSpec]],
 * [[JavascriptFilterSpec]] and [[JavascriptPostAggregationSpec]]
 * @param dqb The DruidQueryBuilder whose ''dimensionSpec'' method will be called.
 * @param e The input expression use to generate js code.
 * @param multiInputParamsAllowed If multiply input parameters allowed for the Javascript function.
 *                                NOTE: This will be true if generate [[JavascriptAggregationSpec]].
 * @param metricAllowed If druid metric will be allowed as the input parameter of the Javascript function.
 *                      NOTE: This will be true if generate [[JavascriptAggregationSpec]].
 * @param timeZone The time zone specified.
 * @param retType The return type of the Javascript function, default to StringType.
 */
case class JSCodeGenerator(dqb: DruidQueryBuilder, e: Expression, multiInputParamsAllowed: Boolean,
                           metricAllowed: Boolean, timeZone: String, retType: DataType = StringType
                          ) extends MyLogging {


  private[this] var vid: Int = 0

  private[jscodegen] def makeUniqueVarName: String = {
    vid += 1
    "v" + vid
  }

  private[jscodegen] val dateTimeCtx = new JSDateTimeCtx(timeZone, this)

  private[this] var inParams: Set[String] = Set()

  def fnParams = inParams.toList

  /**
   * Compose the js function literal.
   * @return Option[(function body, return value)]
   */
  def fnElements: Option[(String, String)] = {
    for (je <- genExprCode(e) if inParams.nonEmpty; // must have a column at least
         rje <- JSCast(je, retType, this).castCode) yield {
      (
        s"""
           ${dateTimeCtx.dateTimeInitCode}
           ${rje.linesSoFar}
         """.stripMargin, rje.getRef)
    }
  }

  def fnCode: Option[String] = {
    for (fn <- fnElements) yield {
      s"""function (${inParams.mkString(", ")}) {
         ${fn._1};
         return (${fn._2});
         }""".stripMargin
    }
  }

  private[jscodegen] def isIntegralNumeric(dt: DataType) = dt match {
    case ShortType | IntegerType | LongType => true
    case _ => false
  }


  private[this] def genExprCode(oe: Any): Option[JSExpr] = {
    if (oe.isInstanceOf[Expression]) {
      val e: Expression = oe.asInstanceOf[Expression]
      e match {
        case AttributeReference(nm, dataType, _, _) =>
          for (dc <- dqb.druidColumn(nm)
               if ((dc.isTimeDimension ||
                 (dc.isMetric && metricAllowed) ||
                 (dc.isDimension() || dc.isNotIndexedDimension)) && validInParams(dc.name))
          ) yield {
            if (dc.isTimeDimension) {
              Some(new JSExpr(DruidDataSource.INNER_TIME_COLUMN_NAME, LongType, true))
            } else if (DruidDataType.sparkDataType(dc.dataType) == dataType) {
              // This is always true because all columns is in Druid.
              Some(new JSExpr(dc.name, dataType, false))
            } else {
              // This seems not happen ??? No !!! The non-indexed dimension
              if (dc.isNotIndexedDimension) {
                log.warn(s"Column '$nm' is not indexed into datasource.")
                Some(new JSExpr(dc.name, StringType, false))
              } else {
                val je = new JSExpr(dc.name, DruidDataType.sparkDataType(dc.dataType), false)
                val cje = JSCast(je, dataType, this).castCode
                cje.map { je => JSExpr(je.fnVar, je.linesSoFar, je.getRef, dataType) }
              }
            }
          }.get // Option[Option].get = Option. This because calling castCode may returns None.
        case Literal(null, dataType) => Some(new JSExpr("null", dataType, false))
        case Literal(v, dataType) =>
          dataType match {
            case ShortType | IntegerType | LongType | FloatType | DoubleType =>
              Some(new JSExpr(v.toString, dataType, false))
            case StringType =>
              Some(new JSExpr(s""""${v.toString}"""", dataType, false))
            case NullType => Some(new JSExpr("null", dataType, false))
            case DateType if v.isInstanceOf[Int] =>
              // days number
              Some(new JSExpr(noDaysToDateCode(v.toString), DateType, false))
            case TimestampType if v.isInstanceOf[Long] =>
              // micro seconds
              Some(new JSExpr(longToISODtCode((v.asInstanceOf[Long] / 1000).toString, dateTimeCtx),
                TimestampType, false))
            case _ => JSCast(new JSExpr(v.toString, StringType, false), dataType, this).castCode
          }

        case Cast(child, dt) => genCastExprCode(child, dt)

        case Concat(exprs) =>
          exprs.tail.foldLeft(genExprCode(exprs.head)) {
            (loje, expr) =>
              for (lje <- loje;
                   je <- genExprCode(expr)) yield {
              JSExpr(None, s"${lje.linesSoFar} + ${je.linesSoFar}",
                s"${lje.getRef}.concat(${je.getRef})", StringType)
            }
          }

        case Upper(u) =>
          // TODO: use UpperAndLowerExtractionFunctionSpec
          for (je <- genExprCode(Cast(u, StringType))) yield
            JSExpr(je.fnVar, je.linesSoFar, s"${je.getRef}.toUpperCase()", StringType)
        case Lower(l) =>
          // TODO: use UpperAndLowerExtractionFunctionSpec
          for (je <- genExprCode(Cast(l, StringType))) yield
            JSExpr(je.fnVar, je.linesSoFar, s"${je.getRef}.toLowerCase()", StringType)

        case Substring(str, pos, len) =>
          // TODO: use SubstringExtractionFunctionSpec
          for (strJe <- genExprCode(Cast(str, StringType));
               posJe <- genExprCode(Cast(pos, IntegerType));
               lenJe <- genExprCode(Cast(len, IntegerType));
               posL = posJe.getRef.toInt if posL >= 0;
               lenL = lenJe.getRef.toInt if lenL > 0) yield {
            JSExpr(None, s"${strJe.linesSoFar} ${posJe.linesSoFar} ${lenJe.linesSoFar}",
              s"(${strJe.getRef}).substr(${posJe.getRef}, ${lenJe.getRef})", StringType)
          }

        case Coalesce(exprs) =>
          val jes = exprs.flatMap(genExprCode(_))
          if (jes.size == exprs.size) {
            Some(jes.foldLeft(JSExpr(None, "", "", StringType)) {
              (lje, je) =>
                JSExpr(None, s"${lje.linesSoFar} ${je.linesSoFar}",
                  if (lje.getRef.isEmpty) s"(${je.getRef})"
                  else s"(${lje.getRef}) || (${je.getRef})", je.fnDT)
            })
          } else None

        case CaseWhenExtract(branches, elseValue) =>
          val le = branches.flatMap(p => Seq(p._1, p._2)) ++ elseValue.toList
          val jes = le.flatMap(genExprCode(_))
          if (jes.size == le.size) {
            val vn = makeUniqueVarName
            val je = (0 until jes.size / 2).foldLeft(JSExpr(None, s"$vn = false;", "", BooleanType)) {
              (lje, i) =>
                val cond = jes(i * 2)
                val value = jes(i * 2 + 1)
                JSExpr(None, lje.linesSoFar +
                  s"""
                     ${cond.linesSoFar};
                     if ($vn != null && !$vn && (${cond.getRef})) {
                       ${value.linesSoFar};
                       $vn = ${value.getRef};
                     }
                   """.stripMargin, "", lje.fnDT)
            }
            if (jes.size % 2 == 1) {
              val value = jes(jes.size - 1)
              Some(JSExpr(None, je.linesSoFar +
                s"""
                   if ($vn != null && !vn) {
                     ${value.linesSoFar};
                     $vn = ${value.getRef};
                   }
                 """.stripMargin, "", je.fnDT))
            } else { // have no else clause
              Some(JSExpr(None, je.linesSoFar +
                s"""
                   if ($vn != null && !vn) {
                     $vn = null;
                   }
                 """.stripMargin, "", je.fnDT))
            }
          } else None

        case If(predicate, trueValue, falseValue) =>
          // TODO: use LooupExtractionFunctionSpec or LookupAggregateSpec
          for (pje <- genExprCode(predicate);
               tje <- genExprCode(trueValue);
               fje <- genExprCode(falseValue)) yield {
            val vn = makeUniqueVarName
            JSExpr(None,
              s"""
                 var $vn = null;
                 ${pje.linesSoFar}
                 ${tje.linesSoFar}
                 ${fje.linesSoFar}
                 if (${pje.getRef}) {
                   $vn = ${tje.getRef};
                 } else {
                   $vn = ${fje.getRef};
                 }
               """.stripMargin, "", tje.fnDT)
          }

        case LessThan(l, r) => genComparisonCode(l, r, " < ")
        case LessThanOrEqual(l, r) => genComparisonCode(l, r, " <= ")
        case GreaterThan(l, r) => genComparisonCode(l, r, " > ")
        case GreaterThanOrEqual(l, r) => genComparisonCode(l, r, " >= ")
        case EqualTo(l ,r) => genComparisonCode(l, r, " == ")
        case And(l, r) => genComparisonCode(l, r, " && ")
        case Or(l, r) => genComparisonCode(l, r, " || ")
        case Not(child) =>
          for (je <- genExprCode(child)) yield
            JSExpr(None, je.linesSoFar, s"!(${je.getRef})", BooleanType)

        case In(value, vlist) if vlist.forall(_.isInstanceOf[Literal]) =>
          val nnvl = vlist.filterNot(v => v.dataType.isInstanceOf[NullType] ||
            v.asInstanceOf[Literal].value == null)
          val nnjes = nnvl.flatMap(genExprCode(_))
          if (nnvl.nonEmpty && nnjes.nonEmpty && nnvl.size == nnjes.size) {
            for (vje <- genExprCode(value)) yield {
              val vlje = nnjes.foldLeft(JSExpr(None, "", "", StringType)) {
                (lje, je) => JSExpr(None, lje.linesSoFar + je.linesSoFar,
                  if (lje.getRef.isEmpty) s"${je.getRef}: true"
                  else s"${lje.getRef}, ${je.getRef}: true", je.fnDT)
              }
              JSExpr(None, vje.linesSoFar + vlje.linesSoFar,
                s"${vje.getRef} in {${vlje.getRef}}", BooleanType)
            }
          } else None

        case InSet(value, vset) if vset.forall(!_.isInstanceOf[Expression]) =>
          // The because the IN optimization would eval each expression in value list.
          val nnvl = vset.filter(_ != null)
          val nnjes = nnvl.flatMap(genExprCode(_))
          if (nnjes.nonEmpty && nnvl.nonEmpty && nnvl.size == nnjes.size) {
            for (vje <- genExprCode(value)) yield {
              val vlje = nnjes.foldLeft(JSExpr(None, "", "", StringType)) {
                (lje, je) => JSExpr(None, lje.linesSoFar + je.linesSoFar,
                  if (lje.getRef.isEmpty) s"${je.getRef}: true"
                  else s"${lje.getRef}, ${je.getRef}: true", je.fnDT)
              }
              JSExpr(None, vje.linesSoFar + vlje.linesSoFar,
                s"${vje.getRef} in {${vlje.getRef}", BooleanType)
            }
          } else None

        case ToDate(child) =>
          for (je <- genExprCode(child); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, cje.getRef, DateType)
        case DateAdd(startDate, days) =>
          for (sdje <- genExprCode(startDate); csdje <- JSCast(sdje, DateType, this).castCode;
               dje <- genExprCode(days); cdje <- JSCast(dje, IntegerType, this).castCode;
               cje <- JSCast(JSExpr(None, csdje.linesSoFar + cdje.linesSoFar,
                 dateAdd(csdje.getRef, cdje.getRef), DateType), StringType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, cje.getRef, StringType)
        case DateSub(startDate, days) =>
          for (sdje <- genExprCode(startDate); csdje <- JSCast(sdje, DateType, this).castCode;
               dje <- genExprCode(days); cdje <- JSCast(dje, IntegerType, this).castCode;
               cje <- JSCast(JSExpr(None, csdje.linesSoFar + cdje.linesSoFar,
                 dateSub(csdje.getRef, cdje.getRef), DateType), StringType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, cje.getRef, StringType)
        case DateDiff(endDate, startDate) =>
          for (edje <- genExprCode(endDate); cedje <- JSCast(edje, DateType, this).castCode;
               sdje <- genExprCode(startDate); csdje <- JSCast(sdje, DateType, this).castCode) yield
              JSExpr(None, cedje.linesSoFar + csdje.linesSoFar,
                dateDiff(cedje.getRef, csdje.getRef), IntegerType)
        case Year(y) =>
          for (je <- genExprCode(y); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, year(cje.getRef), IntegerType)
        case Quarter(q) =>
          for (je <- genExprCode(q); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, year(cje.getRef), IntegerType)
        case Month(m) =>
          for (je <- genExprCode(m); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, month(cje.getRef), IntegerType)
        case DayOfMonth(d) =>
          for (je <- genExprCode(d); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, dayOfMonth(cje.getRef), IntegerType)
        case DayOfYear(d) =>
          for (je <- genExprCode(d); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, dayOfYear(cje.getRef), IntegerType)
        case WeekOfYear(w) =>
          for (je <- genExprCode(w); cje <- JSCast(je, DateType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, weekOfYear(cje.getRef), IntegerType)
        case Hour(h) =>
          for (je <- genExprCode(h); cje <- JSCast(je, TimestampType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, hourOfDay(cje.getRef), IntegerType)
        case Minute(m) =>
          for (je <- genExprCode(m); cje <- JSCast(je, TimestampType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, minuteOfHour(cje.getRef), IntegerType)
        case Second(s) =>
          for (je <- genExprCode(s); cje <- JSCast(je, TimestampType, this).castCode) yield
            JSExpr(cje.fnVar, cje.linesSoFar, secondOfMinute(cje.getRef), IntegerType)

        case FromUnixTime(sec, fmt) =>
          for (sje <- genExprCode(sec); csje <- JSCast(sje, LongType, this).castCode;
               fje <- genExprCode(fmt); cfje <- JSCast(fje, StringType, this).castCode) yield {
            val dtCode = longToISODtCode(csje.getRef, dateTimeCtx)
            JSExpr(None, csje.linesSoFar + cfje.linesSoFar,
              if (fmt.isInstanceOf[Literal]) dtToStrCode(dtCode, fje.getRef)
              else dtToStrCode(dtCode, fje.getRef, false), StringType)
          }

        case UnixTimestamp(time, fmt) =>
          // Because String -> Timestamp cast will call 'stringToISODtCode' method without
          // format string, so we cast to StringType first, then call 'stringToISODtCode'
          // manually with format.
          for (tje <- genExprCode(time); ctje <- JSCast(tje, StringType, this).castCode;
               fje <- genExprCode(fmt); cfje <- JSCast(fje, StringType, this).castCode) yield {
            val longCode = dtToLongCode(stringToISODtCode(tje.getRef, dateTimeCtx, true,
              if (fmt.isInstanceOf[Literal]) true else false, fje.getRef))
            JSExpr(None, ctje.linesSoFar + cfje.linesSoFar, longCode, LongType)
          }

        case TruncDate(date, fmt) =>
          for (dje <- genExprCode(date); cdje <- JSCast(dje, DateType, this).castCode;
               fje <- genExprCode(fmt); cfje <- JSCast(fje, StringType, this).castCode) yield
            JSExpr(None, cdje.linesSoFar + cfje.linesSoFar,
              truncate(cdje.getRef, cfje.getRef), StringType)

        case DateFormatClass(dt, fmt) =>
          for (dje <- genExprCode(dt); cdje <- JSCast(dje, TimestampType, this).castCode;
               fje <- genExprCode(fmt); cfje <- JSCast(fje, StringType, this).castCode) yield
            JSExpr(None, cdje.linesSoFar + cfje.linesSoFar, dtToStrCode(cdje.getRef,
              cfje.getRef, if(fmt.isInstanceOf[Literal]) true else false), StringType)

        case Add(l, r) => genBinaryArithmeticExprCode(l, r, e, "+")
        case Subtract(l, r) => genBinaryArithmeticExprCode(l, r, e, "-")
        case Multiply(l, r) => genBinaryArithmeticExprCode(l, r, e, "*")
        case Divide(l, r) => genBinaryArithmeticExprCode(l, r, e, "/")
        case Remainder(l, r) => genBinaryArithmeticExprCode(l, r, e, "%")
        case Pmod(l, r) =>
          for (lje <- genExprCode(l); clje <- JSCast(lje, castType(l.dataType), this).castCode;
               rje <- genExprCode(r); crje <- JSCast(rje, castType(r.dataType), this).castCode) yield {
            val vn = makeUniqueVarName
            JSExpr(None, clje.linesSoFar + crje.linesSoFar +
                         s"""$vn = null;
                            if (${clje.getRef} > 0) {
                              $vn = Math.abs(${clje.getRef}) % Math.abs(${crje.getRef})
                            } else if (${crje.getRef} > 0) {
                              $vn = ${crje.getRef} - Math.abs(${clje.getRef}) % (${crje.getRef})
                            } else {
                              $vn = -(Math.abs(${clje.getRef}) % Math.abs(${crje.getRef}))
                            }
                          """.stripMargin, "", e.dataType)
          }
        case Abs(child) => genUnaryExprCode(child, e, "Math.abs")
        case Floor(child) => genUnaryExprCode(child, e, "Math.floor")
        case Ceil(child) => genUnaryExprCode(child, e, "Math.ceil")
        case Sqrt(child) => genUnaryExprCode(child, e, "Math.sqrt")
        case Logarithm(Literal(2.718281828459045, DoubleType), child) =>
          genUnaryExprCode(child, e, "Math.log")
        case UnaryMinus(child) => genUnaryExprCode(child, e, "-")
        case UnaryPositive(child) => genUnaryExprCode(child, e, "+")

        case JSStringPredicate(l, r, strFn) =>
          for (lje <- genExprCode(l); clje <- JSCast(lje, StringType, this).castCode;
               rVal <- Some(r.eval()) if rVal != null) yield {
            val s = rVal.asInstanceOf[UTF8String].toString
            JSExpr(None, lje.linesSoFar,
              s"java.lang.String(${lje.getRef}).${strFn}(s)", BooleanType)
          }

        case JSLike(l, r, likeMatch) =>
          for (lje <- genExprCode(l); clje <- JSCast(lje, StringType, this).castCode;
               rVal <- Some(r.eval()) if rVal != null) yield {
            var regexStr = rVal.asInstanceOf[UTF8String].toString
            val reV = makeUniqueVarName
            val v = makeUniqueVarName
            var jsCode: String = null
            if (likeMatch) {
              regexStr = StringEscapeUtils.
                escapeJava(ExprUtil.escapeLikeRegex(regexStr))
              jsCode =
                s"""
                   |var ${reV} = java.util.regex.Pattern.compile("${regexStr}");
                   |var ${v} = ${reV}.matcher(${clje.getRef}).matches();
                 """.stripMargin
            } else {
              jsCode =
                s"""
                   |var ${reV} = java.util.regex.Pattern.compile("${regexStr}");
                   |var ${v} = ${reV}.matcher(${clje.getRef}).find(0);
                 """.stripMargin
            }
            JSExpr(None, clje.linesSoFar + jsCode, "", BooleanType)
          }

        case _ => None
      }
    } else {
      oe match {
        case _: Short => Some(new JSExpr(s"$oe", ShortType, false))
        case _: Int => Some(new JSExpr(s"$oe", IntegerType, false))
        case _: Long => Some(new JSExpr(s"$oe", LongType, false))
        case _: Float => Some(new JSExpr(s"$oe", FloatType, false))
        case _: Double => Some(new JSExpr(s"$oe", DoubleType, false))
        case _: String => Some(new JSExpr(oe.asInstanceOf[String], StringType, false))
        case _ => None
      }
    }
  }

  private[this] def validInParams(inParam: String): Boolean = {
    inParams = inParams + inParam
    if (!multiInputParamsAllowed && inParams.size > 1) false else true
  }

  private[this] def genCastExprCode(e: Expression, dt: DataType): Option[JSExpr] = {
    for (je <- genExprCode(simplifyCast(e, dt));
         cje <- JSCast(je, dt, this).castCode) yield
      JSExpr(cje.fnVar, cje.linesSoFar, cje.getRef, dt)
  }

  private[this] def genComparisonCode(l: Expression, r: Expression, op: String): Option[JSExpr] = {
    for (lje <- genExprCode(l); rje <- genExprCode(r);
         compCode <- genComparisionStr(l, lje, r, rje, op)) yield {
      JSExpr(lje.fnVar.filter(_.nonEmpty).orElse(lje.fnVar),
        lje.linesSoFar + rje.linesSoFar, compCode, BooleanType)
    }
  }

  private[this] def castType(dataType: DataType) = dataType match {
    case ShortType | IntegerType | LongType | FloatType |
         DoubleType | DecimalType() => dataType
    case _ => DoubleType
  }

  private[this] def genBinaryArithmeticExprCode(l: Expression, r: Expression,
                                                c: Expression, op: String): Option[JSExpr] = {
    var op1 = op
    val lCastType = castType(l.dataType)
    val rCastType = castType(r.dataType)

    for (lje <- genExprCode(l); clje <- JSCast(lje, lCastType, this).castCode;
         rje <- genExprCode(r); crje <- JSCast(rje, rCastType, this).castCode) yield
      JSExpr(None, clje.linesSoFar + crje.linesSoFar,
        s"(${clje.getRef}) ${op1} (${crje.getRef})", c.dataType)
  }

  private[this] def genUnaryExprCode(ue: Expression, c: Expression, op: String): Option[JSExpr] = {
    for (je <- genExprCode(ue); cje <- JSCast(je, castType(ue.dataType), this).castCode) yield
      JSExpr(None, cje.linesSoFar, s"$op(${cje.getRef})", c.dataType)
  }

  private[this] def genComparisionStr(l: Expression, lje: JSExpr,
                                      r: Expression, rje: JSExpr, op: String): Option[String] = {
    val ret = if (l.dataType.isInstanceOf[DateType] || l.dataType.isInstanceOf[TimestampType] ||
        r.dataType.isInstanceOf[DateType] || r.dataType.isInstanceOf[TimestampType]) {
      val oclje = JSCast(lje, TimestampType, this).castCode
      val ocrje = JSCast(rje, TimestampType, this).castCode
      for (clje <- oclje; crje <- ocrje) yield {
        dateComparisonCode(clje.getRef, crje.getRef, op)
      }
    } else {
      Some(Some(s"((${lje.getRef}) $op (${rje.getRef}))"))
    }
    ret.flatten
  }

  object JSLike {
    def unapply(pe: Expression): Option[(Expression, Expression, Boolean)] = pe match {
      case Like(l, r) if r.foldable => Some((l, r, true))
      case RLike(l, r) if r.foldable => Some((l, r, false))
      case _ => None
    }
  }

  object JSStringPredicate {
    def unapply(pe: Expression): Option[(Expression, Expression, String)] = pe match {
      case StartsWith(l, r) if r.foldable => Some((l, r, "startsWith"))
      case EndsWith(l, r) if r.foldable => Some((l, r, "endsWith"))
      case Contains(l, r) if r.foldable => Some(l, r, "contains")
      case _ => None
    }
  }

  object CaseWhenExtract {
    def unapply(e: Expression): Option[(Seq[(Expression, Expression)], Option[Expression])] = e match {
      case CaseWhen(branches, elseValue) => Some((branches, elseValue))
      case CaseWhenCodegen(branches, elseValue) => Some((branches, elseValue))
      case _ => None
    }
  }
}
