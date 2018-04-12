package org.rzlabs.druid

class DruidDataSourceException(message: String, cause: Throwable)
    extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}

class QueryGranularityException(message: String, cause: Throwable)
  extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}
