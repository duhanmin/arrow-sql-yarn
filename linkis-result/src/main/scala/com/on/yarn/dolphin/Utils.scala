package com.on.yarn.dolphin

import cn.hutool.core.io.IoUtil
import lombok.extern.slf4j.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

@Slf4j
object Utils {

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  def tryThrow[T](tryOp: => T)(exception: Throwable => Throwable): T = tryCatch(tryOp) {
    t: Throwable => throw exception(t)
  }

  def tryCatch[T](tryOp: => T)(catchOp: Throwable => T): T = {
    try tryOp catch {
      case t: Throwable =>
        log.error("Fatal Error, system exit...", t)
        catchOp(t)
    }
  }

  def tryFinally[T](tryOp: => T)(finallyOp: => Unit): T = try tryOp finally finallyOp

  def getFs(path: String): FileSystem = {
    val conf = new Configuration()
    if (path.startsWith("s3")) {
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }
    FileSystem.get(new Path(path).toUri, conf)
  }

  def closeFs(fs:FileSystem): Unit = {
    if (fs != null) {
      IoUtil.close(fs)
    }
  }
}
