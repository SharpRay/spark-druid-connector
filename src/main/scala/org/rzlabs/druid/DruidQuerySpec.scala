package org.rzlabs.druid

import java.util.Locale

import org.apache.spark.sql.types._
import org.joda.time._

/**
 * As defined in [[http://druid.io/docs/latest/querying/dimensionspecs.html]]
 */
sealed trait DimensionSpec {
  val `type`: String

  def sparkDataType(druidDataSource: DruidDataSource)(implicit dimension: String): DataType = {
    val column: DruidColumn = druidDataSource.columns.getOrElse(dimension,
      throw new DruidDataSourceException(s"Dimension '$dimension' " +
        s"is not existence in DimensionSpec.")
    )
    DruidDataType.sparkDataType(column.dataType)
  }
}

/**
 * Returns dimension values as is and optionally renames the dimension.
 * When specifying a DimensionSpec on a numeric column, the user should
 * include the type of the column in the `outputType` field. If left
 * unspecified, the `outputType` defaults to STRING.
 * @param `type` This string should always be "default".
 * @param dimension The dimension name specified.
 * @param outputName The output dimension alias name specified.
 * @param outputType The output type. This string should be "STRING",
 *                   "LONG" or "FLOAT".
 */
case class DefaultDimensionSpec(`type`: String,
                                dimension: String,
                                outputName: String,
                                outputType: String
                               ) extends  DimensionSpec {

  def this(dimension: String, outputName: String,
           outputType: String) = {
    this("default", dimension, outputName, outputType)
  }

  def this(dimension: String, outputName: String) = {
    this(dimension, outputName, "STRING")
  }

  def this(dimension: String) = {
    this(dimension, dimension)
  }
}

/**
 * Returns dimension values transformed using the given extraction function.
 * When specifying a DimensionSpec on a numeric column, the user should
 * include the type of the column in the `outputType` field. If left
 * unspecified, the `outputType` defaults to STRING.
 * @param `type` This string should always be "extraction".
 * @param dimension The dimension name specified.
 * @param extractionFn The extraction function applied to the dimension
 *                     before aggregation.
 * @param outputName The output dimension alias name specified.
 * @param outputType The output type. This string should be "STRING",
 *                   "LONG" or "FLOAT".
 */
case class ExtractionDimensionSpec(`type`: String,
                                   dimension: String,
                                   extractionFn: ExtractionFunctionSpec,
                                   outputName: String,
                                   outputType: String
                                  ) extends DimensionSpec {

  def this(dimension: String, extractionFn: ExtractionFunctionSpec,
           outputName: String, outputType: String) = {
    this("extraction", dimension, extractionFn, outputName, outputType)
  }

  def this(dimension: String, extractionFn: ExtractionFunctionSpec,
           outputName: String) = {
    this(dimension ,extractionFn, outputName, "STRING")
  }

  def this(dimension: String, extractionFn: ExtractionFunctionSpec) = {
    this(dimension ,extractionFn, dimension)
  }
}

/**
 * These are only useful for multi-value dimensions. If you have a row in Druid that
 * has multi-value dimension with values ["v1","v2","v3"] and you send a groupBy/topN
 * query grouping by that dimension with query filter for value "v1". In the response
 * you will get 3 rows containing "v1", "v2" and "v3". This behavior might be unintuitive
 * for some use cases.
 *
 * It happens because "query filter" is internally used on the bitmaps and only used to
 * match the row to be included in the query result processing. With multi-value dimensions,
 * "query filter" behaves like a contains check, which will match the row with dimension
 * value ["v1","v2","v3"]. Please see the sectio on "Multi-value columns" in segment for
 * more details. Then groupBy/topN processing pipeline "explodes" all multi-value dimensions
 * resulting 3 rows for "v1", "v2" and "v3" each.
 *
 * In addition to "query filter" which efficiently selects the rows to be processed, you can
 * use the filtered dimension spec to filter for specific values within the values of a
 * multi-value dimension. These dimensionSpecs take a delegate DimensionSpec and a filtering
 * criteria. From the "exploded" rows, only rows matching the given filtering criteria are
 * returned in the query result.
 *
 * For more details and examples, see [[http://druid.io/docs/latest/querying/multi-value-dimensions.html]].
 */
sealed trait FilteredDimensionSpec extends DimensionSpec {
  val delegate: DimensionSpec
}

/**
 * @param `type` This string should always be "listFiltered".
 * @param delegate The DimensionSpec delegate for.
 * @param values The wanted values in the multi-value dimension.
 * @param isWhitelist This spec acts as a whitelist (the `values` are wanted)
 *                    or a blacklist (the `values` are not wanted), default is
 *                    as a whitelist (`true` value).
 */
case class ListFilteredDimensionSpec(`type`: String,
                                       delegate: DimensionSpec,
                                       values: List[String],
                                       isWhitelist: Boolean
                                      ) extends FilteredDimensionSpec {

  def this(delegate: DimensionSpec, values: List[String], isWhitelist: Boolean) = {
    this("listFiltered", delegate, values, isWhitelist)
  }

  def this(delegate: DimensionSpec, values: List[String]) = {
    this(delegate, values, true)
  }
}

/**
 * Note that `listFiltered` is faster than this and one should use that
 * for whitelist or blacklist usecase.
 * @param `type` This string should always be "regexFiltered".
 * @param delegate The DimensionSpec delegate for.
 * @param pattern Only values matching the pattern will be chosen
 *                in the multi-value dimension.
 */
case class RegexFilteredDimensionSpec(`type`: String,
                                        delegate: DimensionSpec,
                                        pattern: String
                                       ) extends FilteredDimensionSpec {

  def this(delegate: DimensionSpec, pattern: String) = {
    this("regexFiltered", delegate, pattern)
  }
}

/**
 * This spec used in the [[LimitSpec]].
 * @param dimension Any dimension or metric name.
 * @param direction "ascending" or "descending".
 * @param dimensionOrder "lexcographic", "alphanumeric", "strlen" or "numeric".
 */
case class OrderByColumnSpec(dimension: String,
                             direction: String,
                             dimensionOrder: String) {

  def this(dimension: String, asc: Boolean, order: String) = {
    this(dimension, if (asc) "ascending" else "descending", order)
  }

  def this(dimension: String, asc: Boolean) = {
    this(dimension, asc, "lexicographic")
  }

  def this(dimension: String) = {
    this(dimension, true)
  }
}

/**
 * The limitSpec field provides the functionality to sort and limit the set of results
 * from a groupBy query. If you group by a single dimension and are ordering by a single
 * metric, we highly recommend using TopN Queries instead. The performance will be
 * substantially better.
 * @param `type` This string should always be "default"
 * @param limit A integer value represents the result size to limit.
 * @param columns A list of [[OrderByColumnSpec]].
 */
case class LimitSpec(`type`: String,
                     limit: Int,
                     columns: List[OrderByColumnSpec]) {

  def this(limit: Int, columns: List[OrderByColumnSpec]) = {
    this("default", limit, columns)
  }

  def this(limit: Int, columns: OrderByColumnSpec*) = {
    this("default", limit, columns.toList)
  }

  def this(limit: Int) = this(limit, Nil)
}

/**
 * A having clause is a JSON object identifying which rows from a groupBy query
 * should be returned, by specifying conditions on aggregated values.
 * It is essentially the equivalent of the HAVING clause in SQL.
 * The key in druid query spec is `having`.
 */
sealed trait HavingSpec {
  val `type`: String
}

/**
 * Query filter HavingSpecs allow all Druid query filters to be used in the
 * Having part of the query.
 * @param `type` This string should always be "filter".
 * @param filter The [[FilterSpec]] specified.
 */
case class QueryHavingSpec(`type`: String,
                           filter: FilterSpec) {

  def this(filter: FilterSpec) = {
    this("filter", filter)
  }
}

/**
 *
 * @param `type` This string should be "greaterThan", "equalTo" or "lessThan".
 * @param aggregation The aggregator name.
 * @param value The value for the purposes of comparison with the
 *              aggregation value.
 */
case class NumericHavingSpec(`type`: String,
                             aggregation: String,
                             value: Double
                            ) extends HavingSpec

/**
 * The dimSelector filter will match rows with dimension values equal to the
 * specified value.
 * @param `type` This string should always be "dimSelector".
 * @param dimension The dimension field.
 * @param value The value for the purpose of comparision.
 */
case class DimensionSelectorHavingSpec(`type`: String,
                                       dimension: String,
                                       value: String
                                      ) extends HavingSpec {

  def this(dimension: String, value: String) = {
    this("dimSelector", dimension, value)
  }
}

/**
 * HavingSpecs with logical operators.
 * @param `type` This string should be "and", "or" or "not".
 * @param havingSpecs The [[HavingSpec]] list.
 */
case class LogicalExpressionHavingSpec(`type`: String,
                                       havingSpecs: List[HavingSpec]
                                      ) extends HavingSpec

/**
 * The granularity field determines how data gets bucketed across the time dimension,
 * or how it gets aggregated by hour, day, minute, etc.
 *
 * It can be specified either as a string for simple granularity or as an object for
 * arbitrary granularities.
 *
 * Simple Granularities
 * Simple granularities are specified as a string and bucket timestamps by their UTC
 * time (e.g., days start at 00:00 UTC).
 * Supported granularity strings are: `all`, `none`, `second`, `minute`, `fifteen_minute`,
 * `thirty_minute`, `hour`, `day`, `week`, `month`. `quarter` and `year`.
 *   1. `all` buckets everything into a single bucket(maximum granularity).
 *   2. `none` does not bucket data, it actually uses the (query)granularity of the index -
 *      minimum here is `none` which means millisecond granularity. Using `none` in a
 *      TimeseriesQuery is currently not recommended (the system will try to generate 0 values
 *      for all milliseconds that didn't exist, which is often a lot).
 */
sealed trait GranularitySpec {
  val `type`: String
}

/**
 * All granularity class. Also may be written as string "all".
 * @param `type` This String should always be "all".
 */
case class AllGranularitySpec(`type`: String)
  extends GranularitySpec {

  def this() = this("all")
}

/**
 * None granularity class. Also may be written as string "none".
 * @param `type` This String should always be "none".
 */
case class NoneGranularitySpec(`type`: String)
  extends GranularitySpec {

  def this() = this("none")
}

/**
 * Duration granularities are specified as an exact duration in milliseconds
 * and timestamps are returned as UTC. Duration granularity values are in millis.
 *
 * They also support specifying an optional origin, which defines to start
 * counting time buckets from (defaults to 1970-01-01T00:00:00.000Z).
 *
 * @param `type` This string should always be "duration".
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
    this(duration, null)
  }
}

/**
 * Period granularities are specified as arbitrary period combinations of years,
 * months, weeks, minutes and seconds (e.g. P2W, P3M, PT1H30M, PT0.750S) in
 * ISO8601 format. They support specifying a time zone which determines where
 * period boundaries start as well as the timezone of the returned timestamps.
 * By default, years start on the first of January, months start on the first
 * of the month and weeks start on Mondays unless an origin is specified.
 *
 * Time zone is optional (defaults to UTC). Origin is optional (defauilts to
 * 1970-01-01T00:00:00.000Z in the given timezone).
 *
 * @param `type` This string should always be "period".
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
    this(period, origin, null)
  }

  def this(period: Period) = {
    this(period, null, null)
  }
}

/**
 * A filter is a JSON object indicating which rows of data should be included in the
 * computation for a query. It's essentially the equivalent of the WHERE clause in
 * SQL.
 */
sealed trait FilterSpec {
  val `type`: String
}

/**
 * The simplest filter is a select filter. The selector filter will match a specific
 * dimension with a specific value. Selector filters can be used as the base filters
 * for more complex Boolean expressions of filters.
 * @param `type` This string should always be "selector".
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
    this(dimension, value, null)
  }
}

/**
 * The column comparison filter is similar to the selector filter, but instead
 * compares dimensions to each other.
 * @param `type` This string should always be "columnComparison".
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
    this(dimensions, null)
  }
}

/**
 * The regular expression filter is similar to the selector filter, but using
 * regular expressions. It matches the specified dimension with the given pattern.
 * @param `type` This string should always be "regex"
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
    this(dimension, pattern, null)
  }
}

/**
 * Logical expressions filter.
 * @param `type` This string should be "and", "or" or "not".
 * @param fields The [[FilterSpec]] list.
 */
case class LogicalExpressionFilterSpec(`type`: String,
                                       fields: List[FilterSpec]
                                      ) extends FilterSpec

/**
 * The javascript filter matches a dimension against the specified Javascript
 * function predicate. The filter matches values for which the function
 * returns true.
 * @param `type` This string should always be "javascript".
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
    this(dimension, function, null)
  }
}


/**
 * Extraction filter matches a dimension using some specific Extraction function.
 *
 * NOTE: The extraction filter is now deprecated. The selector filter with an
 * extraction function specified provides identical functionality and should be
 * used instead.
 * @param `type`  This string should always be "extraction"
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
 * @param `type` This string should always be "search".
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
    this(dimension, query, null)
  }
}

/**
 * The in filter can be used to express `IN` operator in SQL.
 * The in filter supports the use of extraction functions.
 * @param `type` This string should always be "in".
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
    this(dimension, values, null)
  }
}

/**
 * Like filters can be used for basic wildcard searches. They are equivalent
 * to the SQL `LIKE` operator. Special characters supported are "%" (matches
 * any number of characters) and "_" (matches any on character).
 * The like filters support the use of extraction functions.
 * @param `type` This string should always be "like".
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
    this(dimension, pattern, null)
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
 * @param `type` This string should always be "bound".
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
    this(dimension, lower, upper, lowerStrict, upperStrict, ordering, null)
  }

  def this(dimension: String, lower: String, upper: String, lowerStrict: Boolean,
           upperStrict: Boolean) = {
    this(dimension, lower, upper, lowerStrict, upperStrict, "lexicographic", null)
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
 * @param `type` This string should always be "interval".
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
    this(dimension, intervals, null)
  }
}

/**
 * Extraction functions define the transformation applied to each dimension value.
 * Transformations can be applied to both regular (string) dimensions, as wel as
 * the special `__time` dimension, which represents the current time bucket according
 * to the query aggregation granularity.
 *
 * Note: for functions taking string values (such as regular expressions), `__time`
 * dimension values will be formatted in ISO-8601 format before getting passed to
 * the extraction function.
 */
sealed trait ExtractionFunctionSpec {
  val `type`: String
}

/**
 * Returns the first matching group for the given regular expression. If there is no match,
 * it returns the dimension value as it is.
 * @param `type` This string should always be "regex".
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
    this(expr, index, replaceMissingValue, null)
  }

  def this(expr: String, index: Int) = {
    this(expr, index, false, null)
  }

  def this(expr: String) = {
    this(expr, 1, false, null)
  }
}

/**
 * Returns the dimension value unchanged if the regular expression matches,
 * otherwise returns null.
 * @param `type` This string should always be "partial".
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
 * @param `type` This string should always be "searchQuery".
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
 * @param `type` This string should always be "substring".
 * @param index The start index.
 * @param length The length the substring to get.
 */
case class SubstringExtractionFunctionSpec(`type`: String,
                                           index: Int,
                                           length: Option[Int]
                                          ) extends ExtractionFunctionSpec {

  def this(index: Int, length: Int) = {
    this("substring", index, if (length > 0) Some(length) else None)
  }

  def this(index: Int) = {
    this(index, 0)
  }

  def this() = {
    this(0)
  }
}

/**
 * Returns the length of dimension values.
 * @param `type` This string should always be "strlen".
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
 * @param `type` This string should always be "timeFormat".
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
                                            timeZone: String,
                                            locale: Locale,
                                            granularity: GranularitySpec,
                                            asMillis: Boolean
                                           ) extends ExtractionFunctionSpec {

  def this(format: String, timeZone: String, locale: Locale,
           granularity: GranularitySpec, asMillis: Boolean) = {
    this("timeFormat", format, timeZone, locale, granularity, asMillis)
  }

  def this(format: String, timeZone: String, locale: Locale,
           granularity: GranularitySpec) = {
    this(format, timeZone, locale, granularity, false)
  }

  def this(format: String, timeZone: String, locale: Locale) = {
    this(format, timeZone, locale, new NoneGranularitySpec)
  }

  def this(format: String, timeZone: String) = {
    this(format, timeZone, Locale.getDefault)
  }

  def this(format: String) = {
    this(format, DateTimeZone.UTC.getID)
  }

  def this() = {
    this(null)
  }
}

/**
 * Parses dimension values as timestamps using the given input format, and
 * returns them formatted using the given output format.
 * Note, if yuo are working with the `__time` dimension, you should consider
 * using the time extraction function instead.
 * @param `type` This string should always be "time"
 * @param timeFormat The origin time format.
 * @param resultFormat The output format.
 */
case class TimeParsingExtractionFunctionSpec(`type`: String,
                                         timeFormat: String,
                                         resultFormat: String
                                        ) extends ExtractionFunctionSpec {
  def this(timeFormat: String, resultFormat: String) {
    this("time", timeFormat, resultFormat)
  }
}

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
 * @param `type` This string should always be "javascript".
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
    this(function, false)
  }
}

/**
 * Lookups are a concept in Druid where dimension values are (optionally) replaced
 * with new values. Explicit lookups allow you to specify a set of keys and values
 * to use when performing the extraction.
 * @param `type` This string should always be "lookup".
 * @param lookup The lookup spec specified the set of keys and values to use when
 *               performing the extraction. Note that the map keys are original
 *               values of dimension and the values are the specified replaced
 *               values.
 * @param retainMissingValue Set this to true will use the dimension's original
 *                           value if it is not found in the lookup. The default
 *                           value is `false`.
 * @param replaceMissingValueWith Set this to `""` has the same effect as setting
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
                                        replaceMissingValueWith: String,
                                        injective: Boolean
                                       ) extends ExtractionFunctionSpec {

  def this(lookup: LookupSpec, retainMissingValue: Boolean,
           replaceMissingValueWith: String, injective: Boolean) = {
    this("lookup", lookup, retainMissingValue, replaceMissingValueWith, injective)
  }

  def this(lookup: LookupSpec, retainMissingValue: Boolean,
           replaceMissingValueWith: String) = {
    this(lookup, retainMissingValue, replaceMissingValueWith, false)
  }

  def this(lookup: LookupSpec, retainMissingValue: Boolean) = {
    this(lookup, retainMissingValue, null, false)
  }

  def this(lookup: LookupSpec) = {
    this(lookup, false, null, false)
  }
}

/**
 * Provides chained execution of extraction functions.
 * @param `type` This string should always be "cascade".
 * @param extractionFns extraction function spec list.
 */
case class CascadeExtractionFunctionSpec(`type`: String,
                                         extractionFns: List[ExtractionFunctionSpec]
                                        ) extends ExtractionFunctionSpec {

  def this(extractionFns: List[ExtractionFunctionSpec]) = {
    this("cascade", extractionFns)
  }

  def this() = this(null)
}

/**
 * Returns the dimension value formatted according to the given format string.
 * @param `type` This string should always be "stringFormat".
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
    this(format, "nullString")
  }

}

/**
 * Returns the dimension values as all upper case or lower case. Optionally user
 * can specify the language to use in order to perform upper or lower transformation.
 * @param `type` This string should be "upper" or "lower".
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
 * @param `type` This string should be "bucket".
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
    this(size, 0)
  }

  def this() = {
    this(1)
  }
}

sealed trait LookupSpec {
  val `type`: String
}

/**
 * The lookup part of `lookup extraction function`.
 * @param `type` This string should always be "map".
 * @param map the map dimension value pairs.
 * @param isOneToOne Is the mapping injective.
 */
case class MapLookupSpec(`type`: String,
                         map: Map[String, String],
                         isOneToOne: Boolean) extends LookupSpec {

  def this(map: Map[String, String], isOneToOne: Boolean) = {
    this("map", map, isOneToOne)
  }

  def this(map: Map[String, String]) = {
    this(map, false)
  }

  def this() = this(null)
}

sealed trait SearchQuerySpec {
  val `type`: String
}

/**
 * @param `type` This string should always be "contains".
 * @param value A String value to run the search over.
 * @param caseSensitive Whether two string should be compared as case sensitive or not.
 */
case class ContainsSearchQuerySpec(`type`: String,
                                   value: String,
                                   caseSensitive: Boolean
                                  ) extends SearchQuerySpec {

  def this(value: String, caseSensitive: Boolean) = {
    this("contains", value, caseSensitive)
  }

  def this(value: String) = {
    this(value, false)
  }
}

/**
 * @param `type` This string should always be "insensitive_contains".
 * @param value A String value to run the search over.
 */
case class InsensitiveContainsSearchQuerySpec(`type`: String,
                                              value: String
                                             ) extends SearchQuerySpec {
  def this(value: String) = {
    this("insensitive_contains", value)
  }
}

/**
 * @param `type` This string should always be "fragment"
 * @param values A List of String values to run the search over.
 * @param caseSensitive Whether strings should be compared as case sensitive or not.
 */
case class FragmentSearchQuerySpec(`type`: String,
                                   values: List[String],
                                   caseSensitive: Boolean
                                  ) extends SearchQuerySpec {

  def this(value: List[String], caseSensitive: Boolean) = {
    this("fragment", value, caseSensitive)
  }

  def this(value: List[String]) = {
    this(value, false)
  }
}

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
 * @param `type` This string should be "longSum", "doubleSum" or "floatSum".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class SumAggregationSpec(`type`: String,
                              name: String,
                              fieldName: String
                             ) extends AggregationSpec

/**
 * computes the minimum of all metric values and Double.POSITIVE_INFINITY
 * @param `type` This string should be "longMin", "doubleMin" or "floatMin".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class MinAggregationSpec(`type`: String,
                              name: String,
                              fieldName: String
                             ) extends AggregationSpec

/**
 * computes the minimum of all metric values and Double.NEGATIVE_INFINITY
 * @param `type` This string should always be "longMax", "doubleMax" or "floatMax".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class MaxAggregationSpec(`type`: String,
                                    name: String,
                                    fieldName: String
                                   ) extends AggregationSpec

/**
 * computes the metric value with the minimum timestamp or 0 if no row exist.
 * @param `type` This string should always be "longFirst", "doubleFirst" or "floatFirst".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class FirstAggregationSpec(`type`: String,
                                  name: String,
                                  fieldName: String
                                 ) extends AggregationSpec

/**
 * computes the metric value with the maximum timestamp or 0 if no row exist.
 * @param `type` This string should always be "longLast", "doubleLast" or "floatLast".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 */
case class LastAggregationSpec(`type`: String,
                               name: String,
                               fieldName: String
                              ) extends AggregationSpec

/**
 * Computes an arbitrary Javascript function over a set of columns (both metrics
 * and dimensions are allowed). Your Javascript functions are expected to return
 * floating-point values.
 * @param `type` This string should always be "javascript".
 * @param name The output metric name.
 * @param fieldNames The input column names.
 * @param fnAggregate The aggregate function.
 * @param fnCombine The combine function.
 * @param fnReset The reset function.
 */
case class JavascriptAggregationSpec(`type`: String,
                                     name: String,
                                     fieldNames: List[String],
                                     fnAggregate: String,
                                     fnCombine: String,
                                     fnReset: String
                                    ) extends AggregationSpec {

  def this(name: String, fieldNames: List[String], fnAggregate: String,
           fnCombine: String, fnReset: String) = {
    this("javascript", name, fieldNames, fnAggregate, fnCombine, fnReset)
  }
}

/**
 * Computes the cardinality of a set of Druid dimensions, using HyperLogLog to estimate
 * the cardinality. Please note that this aggregator will be much slower than indexing
 * a column with the `hyperUnique` aggregator. This aggregator also runs over a dimension
 * column, which means the string dimension cannot be removed from the dataset to improve
 * rollup. In general, we strongly recommend using the hyperUnique aggregator instead of
 * the cardinality aggregator if you do not care about the individual values of a dimenson.
 * @param `type` This string should always be "cardinality".
 * @param name The output metric name.
 * @param fields The dimensions  compute cardinality.
 * @param byRow When setting `byRow` to `false` (the default) it computes the cardinality
 *              of the set composed of the union of all dimension values for all the
 *              given dimensions. For a single dimension, this is equivalent to `SELECT
 *              COUNT(DISTINCT(dimension)) FROM <datasource>`; For multiple dimensions,
 *              this is equivalent to something akin to `SELECT COUNT(DISTINCT(value))
 *              from (SELECT dim_1 as value FROM <datasource> UNION SELECT dim_2 as value
 *              FROM <datasource> UNION SELECT dim_3 as value FROM <datasource>)`.
 *              When setting `byRow` to `true` itw computes the cardinality by row, i.e.
 *              the cardinality of distinct dimenson combinations. This is equivalent to
 *              someing akin to `SELECT COUNT(*) FROM (SELECT DIM1, DIM2, DIM3 FROM
 *              <datasource> GROUP BY DIM1, DIM2, DIM3)`
 * @param round The HyperLogLog algorithm generates decimal estimates with some error.
 *              "round" can be set to true to round off estimated values to while numbers.
 *              Note that event with rounding, the cardinality is still an estimate. The
 *              "round" field only affects query-time behavior, and is ignored at ingest-time.
 */
case class CardinalityAggregationSpec(`type`: String,
                                      name: String,
                                      fields: List[Either[String, DimensionSpec]],
                                      byRow: Boolean,
                                      round: Boolean
                                     ) extends AggregationSpec {

  def this(name: String, fields: List[Either[String, DimensionSpec]],
           byRow: Boolean, round: Boolean) = {
    this("cardinality", name, fields, byRow, round)
  }

  def this(name: String, fields: List[Either[String, DimensionSpec]],
           byRow: Boolean) = {
    this(name, fields, byRow, true)
  }

  def this(name: String, fields: List[Either[String, DimensionSpec]]) = {
    this(name, fields, false)
  }
}

/**
 * Use HyperLogLog to compute the estimated cardinality of a dimension that has been
 * aggregated as a "hyperUnique" metric at indexing time.
 * @param `type` This string should always be "hyperUnique".
 * @param name The output metric name.
 * @param fieldName The input metric name.
 * @param isInputHyperUnique This can be set to true to index pre-computed HLL
 *                           (Base64 encoded output from druid-hll is expected).
 *                           This field onlyu affects ingestion-time behavior,
 *                           and is ignored at query-time.
 * @param round The HyperLogLog algorithm generates decimal esti,ates with some error.
 *              "round" can be set to true to round off estimated values to whole
 *              numbers. Note the even with rounding, the cardinality is still an
 *              estimate. The "round" field only affects query-time behavior, and
 *              is ignored at ingestion-time.
 */
case class HyperUniqueAggregationSpec(`type`: String,
                                      name: String,
                                      fieldName: String,
                                      isInputHyperUnique: Boolean,
                                      round: Boolean
                                     ) extends AggregationSpec {

  def this(name: String, fieldName: String,
           isInputHyperUnique: Boolean, round: Boolean) = {
    this("hyperUnique", name, fieldName, isInputHyperUnique, round)
  }

  def this(name: String, fieldName: String,
           isInputHyperUnique: Boolean) = {
    this(name, fieldName, isInputHyperUnique, true)
  }

  def this(name: String, fieldName: String) = {
    this(name, fieldName, false)
  }
}

/**
 * Druid aggregator based on datasketches library. Note taht sketch algorithms are approximate;
 * see details in the "accuracy" section of the datasketches doc. At ingestion time, this aggre-
 * gator creates the theta sketch objects get stored in Druid segments. Logically speaking, a
 * theta sketch object can be thought of as a Set data structure. At query time, sketches are
 * read and aggregated(set unioned) together. In the end, by default, you receive the estimate of
 * the number of unique entries in the sketch object. Also, you can use post aggregators to do
 * union, intersection or difference on sketch columns in the same row. Note that you can use
 * `thetaSketch` aggregator on columns which were not ingested using same, it will return estimated
 * cardinality of the coumn. It is recommanded to use it at ingestion time as well to make
 * querying faster.
 * To use datasketch aggregators, make sure you include the extension in your config file:
 * `druid.extensions.loadList=["druid-datasketches"]`
 * @param `type` This string should always be "thetaSketch".
 * @param name A string fo the output (result) name of the calculation.
 * @param fieldName A string for the name of the aggregator used at ingestion time.
 * @param isInputThetaSketch This should only be used at indexing time if your input data
 *                           contains theta sketch objects. This would be the case if you
 *                           use datasketches library outside of Druid, say with Pig/Hive,
 *                           to produce the data that you are ingesting into Druid.
 * @param size Must be a power of 2. Internally, size refers to the maximum number of entries
 *             sketch object will return. Higher size means higher accuracy but more space to
 *             store sketches. Note that after you index with a particular size, druid will
 *             persist sketch in segments and you will use size greater or equal to that at
 *             query time. See the DataSketches site for details. In general, we recommend
 *             just sticking to default size.
 */
case class SketchAggregationSpec(`type`: String,
                                 name: String,
                                 fieldName: String,
                                 isInputThetaSketch: Boolean,
                                 size: Int
                                ) extends AggregationSpec {

  def this(name: String, fieldName: String, isInputThetaSketch: Boolean,
           size: Int) = {
    this("thetaSketch", name, fieldName, isInputThetaSketch, size)
  }

  def this(name: String, fieldName: String, isInputThetaSketch: Boolean) = {
    this(name, fieldName, isInputThetaSketch, 16384)
  }

  def this(name: String, fieldName: String) = {
    this(name, fieldName, false)
  }
}

/**
 * A filtered aggregator wraps any given aggregator, but only aggregates the value
 * for which the given dimension filter matches.
 * This makes it possible to compute the results of a filtered and an unfiltered
 * aggregation simultaneously, without having to issue multiple queries, and use
 * both results as part of post-aggregations.
 * Note: If only the filtered results are required, consider putting the filter
 * on the query itself, which will be much faster since it does not require scanning
 * all the data.
 * @param `type` This string should always be "filtered".
 * @param filter The [[FilterSpec]] specified.
 * @param aggregator The [[AggregationSpec]] specified.
 */
case class FilteredAggregationSpec(`type`: String,
                                   filter: FilterSpec,
                                   aggregator: AggregationSpec
                                  ) extends AggregationSpec {
  def this(filter: FilterSpec, aggregator: AggregationSpec) = {
    this("filtered", filter, aggregator)
  }
}

/**
 * Post-aggregations are specifications of processing that should happen on aggregated
 * values as they come out of Druid. If you include a post aggregation as part of a
 * query, make sure to include all aggregations the post-aggregator requires.
 */
sealed trait PostAggregationSpec {
  val `type`: String
}

/**
 * The arithmetic post-aggregator applies the provided function to the given fields from
 * left to right. The fields can be aggregators or other post aggregators.
 * Note: 1. `/` division always returns `0` if dividing by `0`, regardless of the numerator.
 *       2. `quotient` divison behaves like regular floating point division.
 * @param `type` This string should always be "arithmetic".
 * @param name The output metric name.
 * @param fn Supported functions are `+`, `-`, `*`, `/` and `quotient`.
 * @param fields The fields can be aggregators or other post aggregators.
 * @param ordering Defines the order or resulting values when sorting results (this can
 *                 be useful for topN queries for instance):
 *                 1. If no ordering (or `null`) is specified, the default floating point
 *                    ordering is used.
 *                 2. `numericFirst` ordering always returns finite values first, followed
 *                    by `NaN`, and infinite values last.
 */
case class ArithmeticPostAggregationSpec(`type`: String,
                                         name: String,
                                         fn: String,
                                         fields: List[PostAggregationSpec],
                                         ordering: String
                                        ) extends PostAggregationSpec {

  def this(name: String, fn: String, fields: List[PostAggregationSpec],
           ordering: String) = {
    this("arithmetic", name, fn, fields, ordering)
  }

  def this(name: String, fn: String, fields: List[PostAggregationSpec]) = {
    this(name, fn, fields, null)
  }
}

/**
 * These post-aggregators (`fieldAccess`, `finalizingFieldAccess`)
 * returns then value produced by the specified aggregator.
 * @param `type` This string should by "fieldAccess" or "finalizingFieldAccess".
 * @param name The output post-aggregate metric name.
 * @param fieldName Refers to the output name of the aggregator given in the `aggregations`
 *                  portion of the quer. For complex aggregators, like "cardinality" and
 *                  "hyperUnique", the `type` of the post-aggregator determines what the
 *                  post-aggregator will return. Use type "fieldAccess" to return the raw
 *                  aggregation object, or use type "finalizingFieldAccess" to return a
 *                  finalized value, such as an estimated cardinality.
 */
case class FieldAccessorPostAggregationSpec(`type`: String,
                                           name: String,
                                            fieldName: String
                                           ) extends PostAggregationSpec

/**
 * The constant post-aggregator always returns the specified value.
 * @param `type` This string should always be "constant"
 * @param name The output metric name.
 * @param value The numeric value specified.
 */
case class ConstantPostAggregationSpec(`type`: String,
                                       name: String,
                                       value: Double)

/**
 * `doubleGreatest` and `longGreatest` computes the maximum of all fields and
 * Double.NEGATIVE_INFINITY. `doubleLeast` and `longLeast` comptes the minimum
 * of all fields and Double.POSITIVE_INFINITY.
 * The difference between the `doubleMax` aggregator and the `doubleGreatest`
 * post-aggregator is the `doubleMax` returns the highest value of all rows for
 * one specific column while `doubleGreatest` returns the highest value of multiple
 * columns in one row. There are similar to the SQL MAX and GREATEST functions.
 * @param `type` This string should be "doubleGreatest" or "longGreatest".
 * @param name The output metric name.
 * @param fields The post-aggregators specified.
 */
case class GreatestPostAggregationSpec(`type`: String,
                                       name: String,
                                       fields: List[PostAggregationSpec]
                                      ) extends PostAggregationSpec

/**
 * @param `type` This string should be "doubleLeaset" or "longLeast".
 * @param name The output metric name.
 * @param fields The post-aggregators specified.
 */
case class LeastPostAggregationSpec(`type`: String,
                                    name: String,
                                    fields: List[PostAggregationSpec]
                                   ) extends PostAggregationSpec

/**
 * Applies the provided Javascript function to the given fields. Fields are passed
 * as arguments to the Javascript function in the given order.
 * @param `type` This string should always be "javascript".
 * @param name The output metric name.
 * @param fieldNames The list of aggregator names.
 * @param function javascript function with the aggregator names provided by
 *                 `feldNames` argument as the parameters.
 */
case class JavascriptPostAggregationSpec(`type`: String,
                                         name: String,
                                         fieldNames: List[String],
                                         function: String
                                        ) extends PostAggregationSpec {
  def this(name: String, fieldNames: List[String], function: String) = {
    this("javascript", name, fieldNames, function)
  }
}

/**
 * The hyperUniqueCardinality post aggregator is used to wrap a hyperUnique
 * object or a cardinality object such that it can be used in post aggregations.
 * @param `type` This string should always be "hyperUniqueCardinality".
 * @param name The output metric name.
 * @param fieldName The name of the hyperUnique or cardinality aggregator.
 */
case class HyperUniqueCardinalityPostAggregationSpec(`type`: String,
                                                     name: String,
                                                     fieldName: String
                                                    ) extends PostAggregationSpec {

  def this(name: String, fieldName: String) = {
    this("hyperUniqueCardinality", name, fieldName)
  }
}