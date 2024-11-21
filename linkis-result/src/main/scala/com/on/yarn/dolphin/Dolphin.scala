package com.on.yarn.dolphin

import java.io.{IOException, InputStream}

object Dolphin {

  val CHAR_SET = "utf-8"
  val MAGIC = "dolphin"

  val MAGIC_BYTES = MAGIC.getBytes("utf-8")
  val MAGIC_LEN = MAGIC_BYTES.length

  val DOLPHIN_FILE_SUFFIX = ".dolphin"

  val COL_SPLIT = ","
  val COL_SPLIT_BYTES = COL_SPLIT.getBytes("utf-8")
  val COL_SPLIT_LEN = COL_SPLIT_BYTES.length

  val NULL = "NULL"
  val NULL_BYTES = "NULL".getBytes("utf-8")

  val INT_LEN = 10

  val FILE_EMPTY = 31

  /**
   * Read an integer value that converts the array to a byte of length 10 bytes
   * 读取整数值，该值为将数组转换为10字节长度的byte
   *
   * @param inputStream
   * @return
   */
  def readInt(inputStream: InputStream): Int = {
    val bytes = new Array[Byte](INT_LEN + 1)
    if (readBytes(inputStream, bytes, INT_LEN) != INT_LEN) throw new RuntimeException("读取整数失败")
    getString(bytes, 0, INT_LEN).toInt
  }

  /**
   * Convert a bytes array to a String content
   * 将bytes数组转换为String内容
   *
   * @param bytes
   * @param start
   * @param len
   * @return
   */
  def getString(bytes: Array[Byte], start: Int, len: Int) = new String(bytes, start, len, Dolphin.CHAR_SET)

  def readBytes(inputStream: InputStream, bytes: Array[Byte], len: Int): Int = {
    var count = 0
    while (count < len) {
      val value = inputStream.read()
      if (value == -1 && inputStream.available() < 1) return count
      bytes(count) = value.toByte
      count += 1
    }
    count
  }

  /**
   * Print integers at a fixed length(将整数按固定长度打印)
   *
   * @param value
   * @return
   */
  def getIntBytes(value: Int): Array[Byte] = {
    val str = value.toString
    val res = "0" * (INT_LEN - str.length) + str
    Dolphin.getBytes(res)
  }

  def getBytes(value: Any): Array[Byte] = {
    value.toString.getBytes(CHAR_SET)
  }

  def getType(inputStream: InputStream): String = {
    val bytes = new Array[Byte](100)
    val len = readBytes(inputStream, bytes, Dolphin.MAGIC_LEN + INT_LEN)
    if (len == -1) return null
    getType(Dolphin.getString(bytes, 0, len))
  }

  def getType(content: String): String = {
    if (content.length < MAGIC.length || content.substring(0, MAGIC.length) != MAGIC) throw new IOException(s"File header type must be dolphin,content:$content is not")
    content.substring(MAGIC.length, MAGIC.length + INT_LEN).toInt.toString
  }

  def getResultSetPath(parentDir: String, fileName: String): String = {
    val file = if (fileName.startsWith("_")) fileName else "_" + fileName
    val path = if (parentDir.endsWith("/"))
      parentDir + file + DOLPHIN_FILE_SUFFIX
    else
      parentDir + "/" + file + DOLPHIN_FILE_SUFFIX
    path
  }
}
