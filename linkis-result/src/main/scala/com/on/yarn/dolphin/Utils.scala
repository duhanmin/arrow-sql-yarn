/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.on.yarn.dolphin

import cn.hutool.core.io.IoUtil
import lombok.extern.slf4j.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by enjoyyin on 2018/1/10.
 */
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
