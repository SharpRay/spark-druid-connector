package org.rzlabs.druid

import java.util.Locale

import org.apache.spark.sql.types.{DataType, StringType}
import org.joda.time.{DateTime, DateTimeZone, Interval, Period}

/**
 * As defined in [[http://druid.io/docs/latest/querying/dimensionspecs.html]]
 */
sealed trait DimensionSpec {
  val `type`: String
  val dimension: String
  val outputName: String
//  val outputType: DataType

  def sparkDataType(druidDataSource: DruidDataSource): DataType = StringType
}

object DimensionOrderType extends Enumeration {
  val LEXICOGRAPHIC = Value("lexicographic")
  val ALPHANUMERIC = Value("alphanumeric")
  val STRLEN = Value("strlen")
  val NUMERIC = Value("numeric")
}

case class OrderByColumnSpec(dimension: String,
                             direction: String,
                             dimensionOrder: DimensionOrderType.Value = DimensionOrderType.LEXICOGRAPHIC) {

  def this(dimension: String, asc: Boolean) = {
    this(dimension, if (asc) "ascending" else "descending")
  }

  def this(dimension: String) = {
    this(dimension, "ascending")
  }
}

/**
 * As defined in [[http://druid.io/docs/latest/querying/limitspec.html]]
 * The key in druid query spec is `limitSpec`.
 *
 * @param `type`
 * @param limit
 * @param columns
 */
case class LimitSpec(`type`: String,
                     limit: Int,
                     columns: List[OrderByColumnSpec] = Nil) {

  def this(limit: Int, columns: List[OrderByColumnSpec]) = {
    this("default", limit, columns)
  }

  def this(limit: Int, columns: OrderByColumnSpec*) = {
    this("default", limit, columns.toList)
  }
}

/**
 * As defined in [[http://druid.io/docs/latest/querying/having.html]]
 * The key in druid query spec is `having`.
 */
sealed trait HavingSpec {
  val `type`: String
}

//case class QueryHavingSpec

/**
 *
 * @param `type` Type is "greaterThan", "equalTo" or "lessThan"
 * @param aggregation The aggregation field output name
 * @param value The comparative value
 */
case class NumericHavingSpec(`type`: String,
                             aggregation: String,
                             val value: Double) extends HavingSpec


/**
 *
 * @param `type` Type is "dimSelector"
 * @param dimension The dimension field
 * @param value The comparative dimension value
 */
case class DimensionSelectorHavingSpec(`type`: String,
                                       dimension: String,
                                       value: String) extends HavingSpec {

  def this(dimension: String, value: String) = {
    this("dimSelector", dimension, value)
  }
}

/**
 *
 * @param `type` Type is "and", "or" or "not"
 * @param havingSpecs The [[HavingSpec]] list.
 */
case class LogicalExpressionHavingSpec(`type`: String,
                                       havingSpecs: List[HavingSpec]) extends HavingSpec

/**
 * As defined in [[http://druid.io/docs/latest/querying/granularities.html]]
 * The key in druid query spec is `granularity`
 */
sealed trait GranularitySpec {
  val `type`: String
}

/**
 *
 * @param `type` This String should always be "all".
 */
case class AllGranularitySpec(`type`: String) extends GranularitySpec {

  def this() = this("all")
}

/**
 *
 * @param `type` This String should always be "none".
 */
case class NoneGranularitySpec(`type`: String) extends GranularitySpec {

  def this() = this("none")
}

/**
 *
 * @param `type` This String should always be "duration".
 * @param duration The real duration millis.
 * @param origin The origin time.
 */
case class DurationGranularitySpec(`type`: String,
                               duration: Long,
                               origin: DateTime) extends GranularitySpec {

  def this(duration: Long, origin: DateTime) = {
    this("duration", duration, origin)
  }

  def this(duration: Long) = {
    this("duration", duration, null)
  }
}

/**
 *
 * @param `type` This String should always be "period".
 * @param period The real duration in ISO8091 period date format.
 * @param origin The origin time.
 * @param timeZone The time zone, default is UTC.
 */
case class PeriodGranularitySpec(`type`: String,
                             period: Period,
                             origin: DateTime,
                             timeZone: DateTimeZone) extends GranularitySpec {

  def this(period: Period, origin: DateTime, timeZone: DateTimeZone) = {
    this("period", period, origin, timeZone)
  }

  def this(period: Period, origin: DateTime) = {
    this("period", period, origin, null)
  }

  def this(period: Period) = {
    this("period", period, null, null)
  }
}

/**
 * As defined in [[http://druid.io/docs/latest/querying/filters.html]]
 */
sealed trait FilterSpec {
  val `type`: String
}

/**
 * The simplest filter is a select filter. The selector filter will match a specific
 * dimension with a specific value. Selector filters can be used as the base filters
 * for more complex Boolean expressions of filters.
 * @param `type` This String should always be "selector".
 * @param dimension The dimension specified.
 * @param value The value specified.
 * @param extractionFn The extraction function should be used on the dimension values.
 */
case class SelectorFilterSpec(`type`: String,
                              dimension: String,
                              value: String,
                              extractionFn: ExtractionFunctionSpec
                             ) extends FilterSpec {

  def this(dimension: String, value: String,
           extractionFn: ExtractionFunctionSpec) = {
    this("selector", dimension, value, extractionFn)
  }

  def this(dimension: String, value: String) = {
    this("selector", dimension, value, null)
  }
}

/**
 * The column comparison filter is similar to the selector filter, but instead
 * compares dimensions to each other.
 * @param `type` This String should always be "columnComparison".
 * @param dimensions The two dimensions to be compared.
 */
case class ColumnComparisonFilterSpec(`type`: String,
                                      dimensions: (String, String),
                                      extractionFn: ExtractionFunctionSpec
                                     ) extends FilterSpec {

  def this(dimensions: (String, String),
           extractionFn: ExtractionFunctionSpec) = {
    this("columnComparison", dimensions, extractionFn)
  }

  def this(dimensions: (String, String)) = {
    this("columnComparison", dimensions, null)
  }
}

/**
 * The regular expression filter is similar to the selector filter, but using
 * regular expressions. It matches the specified dimension with the given pattern.
 * @param `type` This String should always be "regex"
 * @param dimension The dimension specified.
 * @param pattern The regex pattern specified.
 */
case class RegularExpressionFilterSpec(`type`: String,
                                       dimension: String,
                                       pattern: String,
                                       extractionFn: ExtractionFunctionSpec
                                      ) extends FilterSpec {

  def this(dimension: String, pattern: String,
           extractionFn: ExtractionFunctionSpec) = {
    this("regex", dimension, pattern, extractionFn)
  }

  def this(dimension: String, pattern: String) = {
    this("regex", dimension, pattern, null)
  }
}

/**
 * Logical expressions filter.
 * @param `type` This String should be "and", "or" or "not".
 * @param fields The [[FilterSpec]] list.
 */
case class LogicalExpressionFilterSpec(`type`: String,
                                       fields: List[FilterSpec]
                                      ) extends FilterSpec

/**
 * The javascript filter matches a dimension against the specified Javascript
 * function predicate. The filter matches values for which the function
 * returns true.
 * @param `type` This String should always be "javascript".
 * @param dimension The dimension specified.
 * @param function The javascript function literal specified.
 */
case class JavascriptFilterSpec(`type`: String,
                                dimension: String,
                                function: String,
                                extractionFn: ExtractionFunctionSpec
                               ) extends FilterSpec {

  def this(dimension: String, function: String,
          extractionFn: ExtractionFunctionSpec) = {
    this("javascript", dimension, function, extractionFn)
  }

  def this(dimension: String, function: String) = {
    this("javascript", dimension, function, null)
  }
}


/**
 * Extraction filter matches a dimension using some specific Extraction function.
 *
 * NOTE: The extraction filter is now deprecated. The selector filter with an
 * extraction function specified provides identical functionality and should be
 * used instead.
 * @param `type`  This String should always be "extraction"
 * @param dimension The dimension the extraction function against.
 * @param value The value to be compared with the dimension values
 *              after the extraction function applied on.
 * @param extractionFn The extraction function spec which should be
 *                     applied on the dimension values.
 */
case class ExtractionFilterSpec(`type`: String,
                                dimension: String,
                                value: String,
                                extractionFn: ExtractionFunctionSpec
                               ) extends FilterSpec {

  def this(dimension: String, value: String,
           extractionFn: ExtractionFunctionSpec) = {
    this("extraction", dimension, value, extractionFn)
  }
}

/**
 * Search filters can be used to filter on partial string matches.
 * @param `type` This String should always be "search".
 * @param dimension The dimension specified.
 * @param query The [[SearchQuerySpec]] specified,
 *              may be [[ContainsSearchQuerySpec]],
 *              [[InsensitiveContainsSearchQuerySpec]], or
 *              [[FragmentSearchQuerySpec]].
 */
case class SearchFilterSpec(`type`: String,
                            dimension: String,
                            query: SearchQuerySpec,
                            extractinoFn: ExtractionFunctionSpec
                           ) extends FilterSpec {

  def this(dimension: String, query: SearchQuerySpec,
           extractionFn: ExtractionFunctionSpec) = {
    this("search", dimension, query, extractionFn)
  }

  def this(dimension: String, query: SearchQuerySpec) = {
    this("search", dimension, query, null)
  }
}

/**
 * The in filter can be used to express `IN` operator in SQL.
 * The in filter supports the use of extraction functions.
 * @param `type` This String should always be "in".
 * @param dimension The dimension spedicied.
 * @param values The value specified.
 * @param extractionFn The extraction function used on the
 *                     dimension values.
 */
case class InFilterSpec(`type`: String,
                        dimension: String,
                        values: List[String],
                        extractionFn: ExtractionFunctionSpec
                       ) extends FilterSpec {

  def this(dimension: String, values: List[String],
           extractionFn: ExtractionFunctionSpec) = {
    this("in", dimension, values, extractionFn)
  }

  def this(dimension: String, values: List[String]) = {
    this("in", dimension, values, null)
  }
}

/**
 * Like filters can be used for basic wildcard searches. They are equivalent
 * to the SQL `LIKE` operator. Special characters supported are "%" (matches
 * any number of characters) and "_" (matches any on character).
 * The like filters support the use of extraction functions.
 * @param `type` This String should always be "like".
 * @param dimension The dimension specified.
 * @param pattern The pattern specified.
 * @param extractionFn The extraction function used on the dimension values.
 */
case class LikeFilterSpec(`type`: String,
                          dimension: String,
                          pattern: String,
                          extractionFn: ExtractionFunctionSpec
                         ) extends FilterSpec {

  def this(dimension: String, pattern: String,
           extractionFn: ExtractionFunctionSpec) = {
    this("like", dimension, pattern, extractionFn)
  }

  def this(dimension: String, pattern: String) = {
    this("like", dimension, pattern, null)
  }
}

/**
 * Bound filters support the use of extraction function.
 *
 * The following bound filter expresses the condition `21 <= age <= 31`:
 * `json {"type": "bound", "dimension": "age", "lower": "21", "uper": "31", "ordering": "numeric"}`
 *
 * This filter expresses the condition `foo <= name <= hoo`, using the default lexicographic
 * sorting order. `json {"type": "bound", "dimension": "name", "lower": "foo", "upper": "hoo"}`
 *
 * Using strict bounds, this filter expresses the condition `21 < age < 31`: `json {"type": "bound",
 * "dimension": "age", "lower": "21", "lowerStrict": true, "upper": "31", "upperStrict": true,
 * "ordering": "numeric"}`
 *
 * The user can also specify a one-sided bound by ommit "upper" or "lower". This filter expresses
 * `age < 31`: `json {"type": "bound", "dimension": "age", "upper": "31", "upperStrict": true,
 * "ordering": "numeric"}`
 *
 * Likewise, this filter expresses `age >= 10`: `json {"type": "bnound", "dimension": "age",
 * "lower": "10", "ordering": "numeric"}`
 *
 * @param `type` This String should always be "bound".
 * @param dimension The dimension to filter on.
 * @param lower The lower bound for the filter.
 * @param upper The upper bound for the filter.
 * @param lowerStrict Perform strict comparison on the lower bound ("<" instead of "<=". default false).
 * @param upperStrict Perform strict comparison on the upper bound (">" instead of ">=". default false).
 * @param ordering Specifies the sorting order to use when comparing vaues against the bound. Can be
 *                 one of the following values: `lexicographic`, `alphanumeric`, `numeric`, `strlen`.
 * @param extractionFn The extraction function used on the dimension values.
 */
case class BoundFilterSpec(`type`: String,
                           dimension: String,
                           lower: String,
                           upper: String,
                           lowerStrict: Boolean,
                           upperStrict: Boolean,
                           ordering: String,
                           extractionFn: ExtractionFunctionSpec
                          ) extends FilterSpec {

  def this(dimension: String, lower: String, upper: String, lowerStrict: Boolean,
           upperStrict: Boolean, ordering: String, extractionFn: ExtractionFunctionSpec) = {
    this("bound", dimension, lower, upper, lowerStrict, upperStrict, ordering, extractionFn)
  }

  def this(dimension: String, lower: String, upper: String, lowerStrict: Boolean,
           upperStrict: Boolean, ordering: String) = {
    this("bound", dimension, lower, upper, lowerStrict, upperStrict, ordering, null)
  }

  def this(dimension: String, lower: String, upper: String, lowerStrict: Boolean,
           upperStrict: Boolean) = {
    this("bound", dimension, lower, upper, lowerStrict, upperStrict, "lexicographic", null)
  }
}

/**
 * The interval filter enables range filtering on columns that contain long millisecond
 * values, with the boundaries specified as ISO8601 time intervals. It  is suitable for
 * the `__time` column, long metric columns, and dimensions with values that can be parsed
 * as long milliseconds.
 * This filter converts the ISO 8601 intervals to long millisecond start/end ranges and
 * translates to an OR of Bound filters on those millisecond ranges, with numeric comparison.
 * The Bound filters will have left-closed and right-open matching (i.e. start <= time < end).
 * @param `type` This String shoud always be "interval".
 * @param dimension The dimension to filter on.
 * @param intervals A list containing ISO-8601 interval strings. This defines the time ranges
 *                  to filter on.
 * @param extractionFn Extraction function to apply to the dimension.
 */
case class IntervalFilterSpec(`type`: String,
                              dimension: String,
                              intervals: List[Interval],
                              extractionFn: ExtractionFunctionSpec
                             ) extends FilterSpec {

  def this(dimension: String, intervals: List[Interval],
           extractionFn: ExtractionFunctionSpec) = {
    this("interval", dimension, intervals, extractionFn)
  }

  def this(dimension: String, intervals: List[Interval]) = {
    this("interval", dimension, intervals, null)
  }
}

sealed trait ExtractionFunctionSpec {
  val `type`: String
}

/**
 * Returns the first matching group for the given regular expression. If there is no match,
 * it returns the dimension value as it is.
 * @param `type` This String should always be "regex".
 * @param expr The regular expression to run the extraction over.
 * @param index The group to extract, default 1.
 *              Index zero extracts the string matching the entire pattern.
 * @param replaceMissingValue If true, the extraction function will transform dimension
 *                            values that do not match the regex pattern to a
 *                            user-specified String. Default is `false`.
 * @param replaceMissingValueWith This value sets the String that unmatched dimension values
 *                                will be replaced with, if `replaceMissingValue` is true.
 *                                If this field is specified as null, unmatched dimension values
 *                                will be replaced with nulls.
 */
case class RegularExpressionExtractionFunctionSpec(`type`: String,
                                                   expr: String,
                                                   index: Int,
                                                   replaceMissingValue: Boolean,
                                                   replaceMissingValueWith: String
                                                  ) extends ExtractionFunctionSpec {

  def this(expr: String, index: Int, replaceMissingValue: Boolean,
           replaceMissingValueWith: String) = {
    this("regex", expr, index, replaceMissingValue, replaceMissingValueWith)
  }

  def this(expr: String, index: Int, replaceMissingValue: Boolean) = {
    this("regex", expr, index, replaceMissingValue, null)
  }

  def this(expr: String, index: Int) = {
    this("regex", expr, index, false, null)
  }

  def this(expr: String) = {
    this("regex", expr, 1, false, null)
  }
}

/**
 * Returns the dimension value unchanged if the regular expression matches,
 * otherwise returns null.
 * @param `type` This String should always be "partial".
 * @param expr The regular expression to run the extraction over.
 */
case class PartialExtractionFunctionSpec(`type`: String,
                                         expr: String
                                        ) extends ExtractionFunctionSpec {

  def this(expr: String) = {
    this("partial", expr)
  }
}

/**
 * Returns the dimension value unchanged if the given `SearchQuerySpec` matches,
 * otherwise returns null.
 * @param `type` This String should always be "searchQuery".
 * @param searchQuerySpec The concrete `SearchQuerySpec`.
 */
case class SearchQueryExtractionFunctionSpec(`type`: String,
                                             searchQuerySpec: SearchQuerySpec
                                            ) extends ExtractionFunctionSpec {

  def this(searchQuerySpec: SearchQuerySpec) = {
    this("searchQuery", searchQuerySpec)
  }
}

/**
 * Returns a substring of the dimension value starting from the supplied index and
 * of hte desired length.
 * @param `type` This String should always be "substring".
 * @param index The start index.
 * @param length The length the substring to get.
 */
case class SubstringExtractionFunctionSpec(`type`: String,
                                           index: Int,
                                           length: Option[Int]
                                          ) extends ExtractionFunctionSpec {

  def this(index: Int, length: Int) = {
    this("substring", index, Some(length))
  }

  def this(index: Int) = {
    this("substring", index, None)
  }

  def this() = {
    this("substring", 0, None)
  }
}

/**
 * Returns the length of dimension values.
 * @param `type` This String should always be "strlen".
 */
case class StrlenExtractionFunctionSpec(`type`: String
                                       ) extends ExtractionFunctionSpec {

  def this() = this("strlen")
}

/**
 * Returns the dimension value formatted according to the given
 * format string, time zone, and locale.
 * For `__time` dimension values, this formats the time value
 * bucketed by the aggregation granularity.
 * @param `type` This String should always be "timeFormat".
 * @param format Data time format for the resulting dimension value,
 *               in joda Time String, or null to use the
 *               default ISO8061 format.
 * @param timeZone Time zone to use, e.g Asia/Shanghai, default UTC.
 * @param locale Locale to use, default current locale.
 * @param granularity Granularity to apply before formatting, or omit
 *                    to not apply any granularity.
 * @param asMillis Boolean value, set to true to treat input strings as
 *                 millis rather than ISO8061 strings.
 */
case class TimeFormatExtractionFunctionSpec(`type`: String,
                                            format: String,
                                            timeZone: DateTimeZone,
                                            locale: Locale,
                                            granularity: GranularitySpec,
                                            asMillis: Boolean
                                           ) extends ExtractionFunctionSpec {

  def this(format: String, timeZone: DateTimeZone, locale: Locale,
           granularity: GranularitySpec, asMillis: Boolean) = {
    this("timeFormat", format, timeZone, locale, granularity, asMillis)
  }

  def this(format: String, timeZone: DateTimeZone, locale: Locale,
           granularity: GranularitySpec) = {
    this("timeFormat", format, timeZone, locale, granularity, false)
  }

  def this(format: String, timeZone: DateTimeZone, locale: Locale) = {
    this("timeFormat", format, timeZone, locale, new NoneGranularitySpec, false)
  }

  def this(format: String, timeZone: DateTimeZone) = {
    this("timeFormat", format, timeZone, Locale.getDefault, new NoneGranularitySpec, false)
  }

  def this(format: String) = {
    this("timeFormat", format, DateTimeZone.UTC, Locale.getDefault, new NoneGranularitySpec, false)
  }

  def this() = {
    this("timeFormat", null, DateTimeZone.UTC, Locale.getDefault, new NoneGranularitySpec, false)
  }
}

/**
 * Parses dimension values as timestamps using the given input format, and
 * returns them formatted using the given output format.
 * Note, if yuo are working with the `__time` dimension, you should consider
 * using the time extraction function instead.
 * @param `type` This String should always be "time"
 * @param timeFormat The origin time format.
 * @param resultFormat The output format.
 */
case class TimeParsingExtractionFunctionSpec(`type`: String,
                                         timeFormat: String,
                                         resultFormat: String
                                        ) extends ExtractionFunctionSpec

/**
 * Returns the dimension value, as transformed by the given javascript function.
 * For regular dimensions, the input value is passed as a string.
 * For the `__time` dimension, the input value is passed  as a number representing
 * the number of milliseconds since Janurary 1, 1970 UTC.
 *
 * NOTE: javascript-based functionality is disabled by default, you can enable it
 * by setting the configuration property `druid.javascript.enabled = true` in
 * `_common/common.runtime.properties` configuration file.
 * Please see [[http://druid.io/docs/latest/development/javascript.html]]
 *
 * @param `type` This String should always be "javascript".
 * @param function The javascript function applied to some dimension.
 * @param injective Specifies if the javascript function preserves uniqueness, default `false`.
 */
case class JavascriptExtractionFunctionSpec(`type`: String,
                                        function: String,
                                        injective: Boolean
                                       ) extends ExtractionFunctionSpec {

  def this(function: String, injective: Boolean) = {
    this("javascript", function, injective)
  }

  def this(function: String) = {
    this("javascript", function, false)
  }
}

/**
 * Lookups are a concept in Druid where dimension values are (optionally) replaced
 * with new values. Explicit lookups allow you to specify a set of keys and values
 * to use when performing the extraction.
 * @param `type` This String should always be "lookup".
 * @param lookup The lookup spec specified the set of keys and values to use when
 *               performing the extraction. Note that the map keys are original
 *               values of dimension and the values are the specified replaced
 *               values.
 * @param retainMissingValue Set this to true will use the dimension's original
 *                           value if it is not found in the lookup. The default
 *                           value is `false`.
 * @param repalceMissingValueWith Set this to `""` has the same effect as setting
 *                                it to `null` or omitting the property. The default
 *                                value is `null`.
 *                                NOTE: It is illegal to set `retainMissingValue = true`
 *                                and also specify a `replaceMissingValueWith`.
 * @param injective This specifies if optimizations can be used which assume there is
 *                  no combing of multiple names into one.
 *                  NOTE: Setting this value to true and setting `retainMissingValue`
 *                  to false (the default) may cause undesired behavior.
 */
case class LookupExtractionFunctionSpec(`type`: String,
                                        lookup: LookupSpec,
                                        retainMissingValue: Boolean,
                                        repalceMissingValueWith: String,
                                        injective: Boolean
                                       ) extends ExtractionFunctionSpec {

  def this(lookup: LookupSpec, retainMissingValue: Boolean,
           replaceMissingValueWith: String, injective: Boolean) = {
    this("lookup", lookup, retainMissingValue, replaceMissingValueWith, injective)
  }

  def this(lookup: LookupSpec, retainMissingValue: Boolean,
           replaceMissingValueWith: String) = {
    this("lookup", lookup, retainMissingValue, replaceMissingValueWith, false)
  }

  def this(lookup: LookupSpec, retainMissingValue: Boolean) = {
    this("lookup", lookup, retainMissingValue, null, false)
  }

  def this(lookup: LookupSpec) = {
    this("lookup", lookup, false, null, false)
  }
}

/**
 * Provides chained execution of extraction functions.
 * @param `type` This String should always be "cascade".
 * @param extractionFns extraction function spec list.
 */
case class CascadeExtractionFunctionSpec(`type`: String,
                                         extractionFns: List[ExtractionFunctionSpec]
                                        ) extends ExtractionFunctionSpec {

  this() = this("cascade")
}

/**
 * Returns the dimension value formatted according to the given format string.
 * @param `type` This String should always be "stringFormat".
 * @param format The format for the dimension value.
 *               For example if you want to concat "[" and "]" before and after
 *               the actual dimension value, you need to specify "[%s]" as
 *               format string.
 * @param nullHanding The handle method of the null value, can be one of
 *                    `nullString`, `emptyString` or `returnNull`. With
 *                    `[%s]` format, each configuration will result
 *                    `[null]`, `[]`, `null`. Default is `nullString`.
 */
case class StringFormatExtractionFunctionSpec(`type`: String,
                                              format: String,
                                              nullHanding: String
                                             ) extends ExtractionFunctionSpec {

  def this(format: String, nullHanding: String) = {
    this("stringFormat", format, nullHanding)
  }

  def this(format: String) = {
    this("stringFormat", format, "nullString")
  }

}

/**
 * Returns the dimension values as all upper case or lower case. Optionally user
 * can specify the language to use in order to perform upper or lower transformation.
 * @param `type` This String should be "upper" or "lower".
 * @param locale
 */
case class UpperAndLowerExtractionFunctionSpec(`type`: String,
                                               locale: String
                                              ) extends ExtractionFunctionSpec {

  def this(`type`: String) = this(`type`, null)
}

/**
 * Bucket extraction function is used to bucket numerical values in each range
 * of the given size by converting then to then same base value.
 * None mumeric values are converted to null.
 * @param `type` This String should be "bucket".
 * @param size The size of the buckets, default 1.
 * @param offset The offset for the buckets, default 0.
 */
case class BucketExtractionSpec(`type`: String,
                                size: Int,
                                offset: Int
                               ) extends ExtractionFunctionSpec {

  def this(size: Int, offset: Int) = {
    this("bucket", size, offset)
  }

  def this(size: Int) = {
    this("bucket", size, 0)
  }

  def this() = {
    this("bucket", 1, 0)
  }
}

sealed trait LookupSpec {
  val `type`: String
}

/**
 * The lookup part of `lookup extraction function`.
 * @param `type` This String should always be "map" ()
 * @param map the map dimension value pairs.
 */
case class MapLookupSpec(`type`: String,
                      map: Map[String, String]) extends LookupSpec {

  def this() = this("map")
}

sealed trait SearchQuerySpec {
  val `type`: String
}

/**
 * @param `type` This String should always be "contains".
 * @param value A String value to run the search over.
 * @param caseSensitive Whether two string should be compared as case sensitive or not.
 */
case class ContainsSearchQuerySpec(`type`: String,
                                   value: String,
                                   caseSensitive: Boolean = false
                                  ) extends SearchQuerySpec

/**
 *
 * @param `type` This String should always be "insensitive_contains".
 * @param value A String value to run the search over.
 */
case class InsensitiveContainsSearchQuerySpec(`type`: String,
                                              value: String
                                             ) extends SearchQuerySpec

/**
 *
 * @param `type` This string should always be "fragment"
 * @param values A List of String values to run the search over.
 * @param caseSensitive Whether strings should be compared as case sensitive or not.
 */
case class FragmentSearchQuerySpec(`type`: String,
                                   values: List[String],
                                   caseSensitive: Boolean = false
                                  ) extends SearchQuerySpec

sealed trait AggregationSpec {
  val `type`: String
}

/**
 * `count` computes the count of Druid rows that match the filters.
 * Please note the count aggregator counts the number of Druid rows,
 * which does not always reflect the number of raw events ingested.
 * This is because Druid can be configured to roll up data at ingestion
 * time. To count the number of ingested rows of data, include a
 * count aggregator at ingestion time, and a longSum aggregator at query
 * time.
 * @param `type` This string should always be "count".
 * @param name The output name.
 */
case class CountAggregationSpec(`type`: String,
                                name: String
                               ) extends AggregationSpec {
  def this(name: String) = {
    this("count", name)
  }
}

/**
 * Computes and stores the sum of values as 64-bit signed integer.
 * @param `type` This string should always be "longSum".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class LongSumAggregationSpec(`type`: String,
                                 name: String,
                                 fieldName: String
                                ) extends AggregationSpec {

  def this(name: String, fieldName: String) = {
    this("longSum", name, fieldName)
  }
}

/**
 * Computes and stores the sum of values as 64-bit floating point value.
 * @param `type` This string should always be "doubleSum".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class DoubleSumAggregationSpec(`type`: String,
                                  name: String,
                                  fieldName: String
                                 ) extends AggregationSpec {

  def this(name: String, fieldName: String) = {
    this("doubleSum", name, fieldName)
  }
}

/**
 * Computes and stores the sum of values as 32-bit floating point vaue.
 * @param `type` This string should always be "floatSum".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class FloatSumAggregationSpec(`type`: String,
                                  name: String,
                                  fieldName: String
                                 ) extends AggregationSpec {

  def this(name: String, fieldName: String) = {
    this("floatSum", name, fieldName)
  }
}
