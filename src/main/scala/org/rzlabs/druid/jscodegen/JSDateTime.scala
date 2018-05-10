package org.rzlabs.druid.jscodegen

private[jscodegen] case class JSDateTimeCtx(val timeZone: String, val ctx: JSCodeGenerator) {

  // v1 = org.joda.time.DateTimeZone.forID("${tz_id}")
  private[jscodegen] val tzVar = ctx.makeUniqueVarName
  // v2 = org.joda.time.format.ISODateTimeFormat.dateTimeParser()
  private[jscodegen] val isoFormatterVar = ctx.makeUniqueVarName
  // v3 = v2.withID(v1)
  private[jscodegen] val isoFormatterWIthTzVar = ctx.makeUniqueVarName

  private[jscodegen] var createJodaTz = false
  private[jscodegen] var createJodaISOFormatter = false
  private[jscodegen] var createJodaISOFormatterWithTz = false

  private[jscodegen] def dateTimeInitCode: String = {
    var dtInitCode = ""
    if (createJodaTz) {
      dtInitCode +=
        s"""var $tzVar = org.joda.time.DateTimeZone.forID("$timeZone");"""
    }
    if (createJodaISOFormatter || createJodaISOFormatterWithTz) {
      dtInitCode +=
        s"var $isoFormatterVar = org.joda.time.format.ISODateTimeFormat.dateTimeParser();"
    }
    if (createJodaISOFormatterWithTz) {
      dtInitCode +=
        s"var $isoFormatterWIthTzVar = $isoFormatterVar.withZone($tzVar);"
    }

    dtInitCode
  }
}

private[jscodegen] object JSDateTimeCtx {
  private val dateFormat = "yyyy-MM-dd"
  private val timestampFormat = "yyyy-MM-dd HH:mm:ss"
  private val mSecsInDay = 86400000

  /**
   * The 'ts' param must be a [[org.joda.time.DateTime]] literal,
   * e.g., 'DateTime.parse("2018-01-01", DateTimeZone.forID("UTC"))'.
   * NOTE: The returned unit is second so divide 1000.
   * @param ts must be a [[org.joda.time.DateTime]] literal.
   * @return
   */
  private[jscodegen] def dtToLongCode(ts: String) = s"Math.floor($ts.getMillis() / 1000)"

  /**
   * The 'dt' param may be a [[org.joda.time.LocalDate]] literal,
   * e.g., ''LocalDate.parse("2018-01-01", format.ISODateTimeFormat.dateTimeParser)''.
   * @param dt must be a [[org.joda.time.LocalDate]] literal.
   * @return
   */
  private[jscodegen] def dateToStrCode(dt: String) = s"""$dt.toString("$dateFormat")"""

  /**
   * The ''ts'' param may be a [[org.joda.time.DateTime]] literal.
   * @param ts may be a [[org.joda.time.DateTime]] literal.
   * @param fmt The timestamp format specified, default is ''yyyy-MM-dd HH:mm:ss''.
   * @return
   */
  private[jscodegen] def dtToStrCode(ts: String,
                                     fmt: String = timestampFormat,
                                     litFmt: Boolean = true) = {
    if (litFmt) {
      s"""$ts.toString("$fmt")"""
    } else {
      s"""$ts.toString($fmt)"""
    }
  }

  private[jscodegen] def longToISODtCode(l: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTz = true
    s"new org.joda.time.DateTime($l, ${ctx.tzVar})"
  }

//  private[jscodegen] def stringToDateCode(s: String, ctx: JSDateTimeCtx) = {
//    ctx.createJodaISOFormatter = true
//    s"""org.joda.time.LocalDate.parse($s, ${ctx.isoFormatterVar})"""
//  }

  private[jscodegen] def stringToDateCode(s: String, ctx: JSDateTimeCtx,
                                      withFmt: Boolean = false,
                                      fmt: Option[String] = None): String = {
    ctx.createJodaTz = true
    ctx.createJodaISOFormatterWithTz = true
    if (!withFmt) {
      s"""org.joda.time.LocalDate.parse($s, ${ctx.isoFormatterVar})"""
    } else {
      s"""org.joda.time.LocalDate.parse($s,
         |org.joda.time.format.DateTimeFormat.forPattern("${fmt.get}").withZone(${ctx.tzVar}))""".stripMargin
    }
  }

  /**
   * The ''ts'' param must be a [[org.joda.time.DateTime]] literal.
   * @param ts
   * @return
   */
  private[jscodegen] def dtToDateCode(ts: String) = {
    s"${ts}.toLocalDate()"
  }

  private[jscodegen] def longToDateCode(ts: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTz = true
    s"org.joda.time.LocalDate($ts, ${ctx.tzVar})"
  }


  private[jscodegen] def stringToISODtCode(s: String, ctx: JSDateTimeCtx,
                                           withFmt: Boolean = false,
                                           fmt: Option[String] = None) = {
    ctx.createJodaTz = true
    ctx.createJodaISOFormatterWithTz = true
    if (!withFmt) {
      s"""org.joda.time.DateTime.parse(($s).replace(" ", "T"), ${ctx.isoFormatterWIthTzVar})"""
    } else {
      s"""org.joda.time.DateTime.parse($s,
         |org.joda.time.format.DateTimeFormat.forPattern("$fmt").withZone(${ctx.tzVar}))""".stripMargin
    }
  }

  /**
   * The ''dt'' param must be a [[org.joda.time.LocalDate]] literal.
   * @param dt
   * @param ctx
   * @return
   */
  private[jscodegen] def localDateToDtCode(dt: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTz = true
    s"$dt.toDateTimeAsStartOfDay(${ctx.tzVar})"
  }

  private[jscodegen] def noDaysToDateCode(d: String) = {
    s"org.joda.time.LocalDate($d * $mSecsInDay)"
  }

  private[jscodegen] def dateComparisonCode(l: String, r: String, op: String) = {
    op match {
      case " < " => Some(s"$l.isBefore($r)")
      case " <= " => Some(s"$l.compareTo($r) <= 0")
      case " > " => Some(s"$l.isAfter($r)")
      case " >= " => Some(s"$l.compareTo($r) >= 0")
      case " = " => Some(s"$l.equals($r)")
      case _ => None
    }
  }

  private[jscodegen] def dateAdd(dt: String, nd: String) = s"$dt.plusDays($nd)"
  private[jscodegen] def dateSub(dt: String, nd: String) = s"$dt.minusDays($nd)"
  private[jscodegen] def dateDiff(ed: String, sd: String) = {
    s"org.joda.time.Days.daysBetween($sd, $ed).getDays()"
  }

  private[jscodegen] def year(dt: String) = s"$dt.getYear()"
  private[jscodegen] def quarter(dt: String) = s"(Math.floor(($dt.getMonthOfYear() - 1) / 3) + 1)"
  private[jscodegen] def month(dt: String) = s"$dt.getMonthOfYear()"
  private[jscodegen] def dayOfMonth(dt: String) = s"$dt.getDayOfMonth()"
  private[jscodegen] def dayOfYear(dt: String) = s"$dt.getDayOfYear()"
  private[jscodegen] def weekOfYear(dt: String) = s"$dt.getWeekOfYear()"
  private[jscodegen] def hourOfDay(dt: String) = s"$dt.getHourOfDay()"
  private[jscodegen] def minuteOfHour(dt: String) = s"$dt.getMinuteOfHour()"
  private[jscodegen] def secondOfMinute(dt: String) = s"$dt.getSecondOfMinute()"

  private[jscodegen] def truncate(dt: String, fmt: String)= fmt.toLowerCase() match {
    case "year" | "yyyy" | "yy" => s"$dt.withDayOfYear(1)"
    case "month" | "mon" | "mm" => s"$dt.withDayOfMonth(1)"
    case _ => "null"
  }
}