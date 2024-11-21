package com.on.yarn.dolphin

import com.on.yarn.dolphin.Utils.getFs
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{IOException, OutputStream}
import scala.collection.mutable.ArrayBuffer

class StorageResultSetWriter(storePath: String, maxCacheSize: Long = 512 * 1024L,dolphin: Int = 2) {

  private val buffer = new ArrayBuffer[Byte]()
  private var moveToWriteRow = false
  private var outputStream: OutputStream = _
  private var rowCount = 0
  private var fs: FileSystem = _

  private var rMetaData: Array[Any] = _

  override def toString: String = {
    if (outputStream == null) {
      if (isEmpty) return ""
      new String(buffer.toArray, Dolphin.CHAR_SET)
    } else {
      storePath
    }
  }

  def isEmpty: Boolean = {
    rMetaData == null && buffer.length <= Dolphin.FILE_EMPTY
  }

  @scala.throws[IOException]
  def addMetaData(metaData: Array[Any]): Unit = {
    if (!moveToWriteRow) {
      {
        rMetaData = metaData
        init()
        if (dolphin == 1) {
          writeLine(strLineToBytes(null))
        } else {
          writeLine(lineToBytes(metaData))
        }
      }
      moveToWriteRow = true
    }
  }

  @scala.throws[IOException]
  def addRecord(record: Array[Any]): Unit = {
    if (moveToWriteRow) {
      rowCount = rowCount + 1
      if (dolphin == 1){
        writeLine(strLineToBytes(record(0).asInstanceOf[String]))
      } else {
        writeLine(lineToBytes(record))
      }
    }
  }

  private def writeLine(bytes: Array[Byte]): Unit = {
    if (buffer.length > maxCacheSize) {
      if (outputStream == null) {
        createNewFile()
      }
      flush()
      outputStream.write(bytes)
    } else {
      buffer.appendAll(bytes)
    }
  }

  def flush(): Unit = {
    createNewFile()
    if (outputStream != null) {
      if (buffer.nonEmpty) {
        outputStream.write(buffer.toArray)
        buffer.clear()
      }
      outputStream.flush()
    }
  }

  private def createNewFile(): Unit = {
    if (storePath != null && outputStream == null) {
      val path = new Path(storePath)
      fs = getFs(storePath)
      outputStream = fs.create(path, true)
    }
  }

  def strLineToBytes(value: String): Array[Byte] = {
    val bytes = if (value == null) Dolphin.NULL_BYTES else Dolphin.getBytes(value)
    Dolphin.getIntBytes(bytes.length) ++ bytes
  }

  /**
   * Convert a row of data to an array of Bytes
   *   * Convert the data to byte and get the corresponding total byte length to write to the file
   *   * Data write format: line length (fixed length) column length (fixed length) field index comma segmentation real data
   *   * For example: 000000004900000000116,10,3,4,5,peace1johnnwang1101true11.51
   *   * The length of the line does not include its own length
   * 将一行数据转换为Bytes的数组
   * 对数据转换为byte，并获取相应的总byte长度写入文件
   * 数据写入格式：行长(固定长度) 列长(固定长度) 字段索引逗号分割 真实数据
   * 如：000000004900000000116,10,3,4,5,peace1johnnwang1101true11.51
   * 其中行长不包括自身长度
   *
   * @param line
   */
  private def lineToBytes(line: Array[Any]): Array[Byte] = {
    //Data cache(数据缓存)
    val dataBytes = ArrayBuffer[Array[Byte]]()
    //Column cache(列缓存)
    val colIndex = ArrayBuffer[Array[Byte]]()
    var colByteLen = 0
    var length = 0
    line.foreach { data =>
      val bytes = if (data == null) Dolphin.NULL_BYTES else Dolphin.getBytes(data)
      dataBytes += bytes
      val colBytes = Dolphin.getBytes(bytes.length)
      colIndex += colBytes += Dolphin.COL_SPLIT_BYTES
      colByteLen += colBytes.length + Dolphin.COL_SPLIT_LEN
      length += bytes.length
    }
    length += colByteLen + Dolphin.INT_LEN
    toByteArray(length, colByteLen, colIndex, dataBytes)
  }

  /**
   * Splice a row of data into a byte array(将一行的数据拼接成byte数组)
   *
   * @param length     The total length of the line data byte, excluding its own length(行数据byte总长度，不包括自身的长度)
   * @param colByteLen Record field index byte column length(记录字段索引byte的列长)
   * @param colIndex   Field index, including separator comma(字段索引，包括分割符逗号)
   * @param dataBytes  Byte of real data(真实数据的byte)
   * @return
   */
  private def toByteArray(length: Int, colByteLen: Int, colIndex: ArrayBuffer[Array[Byte]], dataBytes: ArrayBuffer[Array[Byte]]): Array[Byte] = {
    val row = ArrayBuffer[Byte]()
    colIndex ++= dataBytes
    row.appendAll(Dolphin.getIntBytes(length))
    row.appendAll(Dolphin.getIntBytes(colByteLen))
    colIndex.foreach(row.appendAll(_))
    row.toArray
  }

  def close(): Unit = {
    Utils.tryFinally(if (outputStream != null) flush()) {
      if (outputStream != null) {
        outputStream.close()
      }
      closeFs()
    }
  }

  private def closeFs(): Unit = {
    Utils.closeFs(fs)
  }

  private def init(): Unit = {
    writeLine(Dolphin.MAGIC_BYTES ++ Dolphin.getIntBytes(dolphin))
  }
}