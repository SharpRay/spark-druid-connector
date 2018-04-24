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
    if (createJodaISOFormatter) {
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
  private[jscodegen] def dtToLongCode(ts: String) = s"$ts.getMillis() / 1000"

  /**
   * The 'dt' param may be a [[org.joda.time.LocalDate]] literal,
   * e.g., ''LocalDate.parse("2018-01-01", format.ISODateTimeFormat.dateTimeParser)''.
   * @param dt must be a [[org.joda.time.LocalDate]] literal.
   * @return
   */
  private[jscodegen] def dateToStrCode(dt: String) = s"$dt.toString($dateFormat)"

  /**
   * The ''ts'' param may be a [[org.joda.time.DateTime]] literal.
   * @param ts may be a [[org.joda.time.DateTime]] literal.
   * @param fmt The timestamp format specified, default is ''yyyy-MM-dd HH:mm:ss''.
   * @return
   */
  private[jscodegen] def dtToStrCode(ts: String,
                                     fmt: String = timestampFormat) = {
    s"""$ts.toString("$fmt")"""
  }

  private[jscodegen] def longToISODtCode(l: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTz = true
    s"new org.joda.time.DateTime($l, ${ctx.tzVar})"
  }
}
