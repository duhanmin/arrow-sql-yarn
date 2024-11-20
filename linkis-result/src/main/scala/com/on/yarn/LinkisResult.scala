package com.on.yarn

import cn.hutool.core.collection.CollUtil
import cn.hutool.json.{JSONArray, JSONObject, JSONUtil}
import com.on.yarn.dolphin.Dolphin.getResultSetPath
import com.on.yarn.dolphin.{Column, DataType, StorageResultSetWriter, StringType, Utils}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object LinkisResult {

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  def show(jsonStr: String, storePath: String): Unit = {
    val json: JSONObject = JSONUtil.parseObj(jsonStr)
    val startTime = System.currentTimeMillis()

    for (key <- json.keySet()) {
      val alias: String = key
      val data: JSONArray = json.getJSONArray(key)
      if(CollUtil.isEmpty(data)){
        resultSetWriterNull(alias, storePath, "null")
      } else {
        val columns = data.getJSONObject(0).keySet().map(c => Column(c, StringType, "")).toArray[Column]
        columns.foreach(c => log.info(s"c is ${c.columnName}, comment is ${c.comment}"))
        val metaData = columns.map(_.toArray).reduce((a1, a2) => a1 ++ a2)
        val resultSetPath = getResultSetPath(storePath, alias)
        val writer = new StorageResultSetWriter(resultSetPath)
        writer.addMetaData(metaData)
        Utils.tryThrow({
          for (i <- 0 until data.size()) {
            val item: JSONObject = data.getJSONObject(i)
            val r: Array[Any] = columns.map { c => item.getStr(c.columnName)}.toArray
            writer.addRecord(r)
          }
        }) { t =>
          throw new RuntimeException(s"read record  exception", t)
        }
        log.warn(s"Time taken: ${System.currentTimeMillis() - startTime}, Fetched ${data.size()} row(s).")
        writer.flush()
        writer.close()
      }
    }
  }

  def resultSetWriterNull(alias: String, storePath: String, code: String): Unit = {
    val resultSetPath = getResultSetPath(storePath, alias)
    val writer = new StorageResultSetWriter(resultSetPath)
    val metaArrayBuffer = new ArrayBuffer[Tuple2[String, String]]()
    metaArrayBuffer.add(Tuple2("code", StringType.typeName))
    val columns = metaArrayBuffer.map { c => Column(c._1, DataType.toDataType(c._2), "本页无数据,翻到下一页看吧!!!") }.toArray[Column]
    writer.addMetaData(columns.map(_.toArray).reduce((a1, a2) => a1 ++ a2))
    val r: Array[Any] = columns.indices.map { i => code }.toArray
    writer.addRecord(r)
    writer.flush()
    writer.close()
  }
}