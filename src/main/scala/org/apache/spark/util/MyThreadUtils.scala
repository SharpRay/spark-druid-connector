package org.apache.spark.util

import java.util.concurrent.ThreadPoolExecutor

import org.apache.spark.util

object MyThreadUtils {

  def newDaemonCachedThreadPool(prefix: String, maxThreadNumber: Int,
                                keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    ThreadUtils.newDaemonCachedThreadPool(prefix, maxThreadNumber, keepAliveSeconds)
  }
}
