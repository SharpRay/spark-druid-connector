package org.apache.spark.sql.sources.druid

trait CloseableIterator[+A] extends Iterator[A] {
  def closeIfNeeded(): Unit
}
