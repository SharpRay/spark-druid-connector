package org.apache.spark.sql

import org.apache.spark.internal.Logging

trait MyLogging extends Logging {

  override def logInfo(msg: => String): Unit = {
    super.logInfo(msg)
  }

  override def logDebug(msg: => String): Unit = {
    super.logDebug(msg)
  }

  override def logTrace(msg: => String): Unit = {
    super.logTrace(msg)
  }

  override def logWarning(msg: => String): Unit = {
    super.logWarning(msg)
  }

  override def logError(msg: => String): Unit = {
    super.logError(msg)
  }

  override def logInfo(msg: => String, throwable: Throwable): Unit = {
    super.logInfo(msg, throwable)
  }

  override def logDebug(msg: => String, throwable: Throwable): Unit = {
    super.logDebug(msg, throwable)
  }

  override def logTrace(msg: => String, throwable: Throwable): Unit = {
    super.logTrace(msg, throwable)
  }

  override def logWarning(msg: => String, throwable: Throwable): Unit = {
    super.logWarning(msg, throwable)
  }

  override def logError(msg: => String, throwable: Throwable): Unit = {
    super.logError(msg, throwable)
  }

  def logInfo(msg: => String, arg: Object): Unit = {
    super.log.info(msg, arg)
  }

  def logDebug(msg: => String, arg: Object): Unit = {
    super.log.debug(msg, arg)
  }

  def logTrace(msg: => String, arg: Object): Unit = {
    super.log.trace(msg, arg)
  }

  def logWarning(msg: => String, arg: Object): Unit = {
    super.log.warn(msg, arg)
  }

  def logError(msg: => String, arg: Object): Unit = {
    super.log.error(msg, arg)
  }

}