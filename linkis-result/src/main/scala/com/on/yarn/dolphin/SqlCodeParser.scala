package com.on.yarn.dolphin

import cn.hutool.core.text.CharSequenceUtil
import scala.collection.mutable.ArrayBuffer

object SqlCodeParser {
  val separator = ";"
  val defaultLimit: Int = 5000

  def parse(code: String): Array[String] = {
    val codeBuffer = new ArrayBuffer[String]()

    def appendStatement(sqlStatement: String): Unit = {
      codeBuffer.append(sqlStatement.replaceAll("#semi",";"))
    }

    if (CharSequenceUtil.contains(code, separator)) {
      CharSequenceUtil.splitToArray(code, ";").foreach {
        case s if CharSequenceUtil.isBlank(s) =>
        case s if isSelectCmdNoLimit(s) => appendStatement(s + " limit " + defaultLimit);
        case s => appendStatement(s);
      }
    } else {
      code match {
        case s if CharSequenceUtil.isBlank(s) =>
        case s if isSelectCmdNoLimit(s) => appendStatement(s + " limit " + defaultLimit);
        case s => appendStatement(s);
      }
    }
    codeBuffer.toArray
  }

  def isSelectCmdNoLimit(cmd: String): Boolean = {
    var code = cmd.trim
    if (!cmd.split("\\s+")(0).equalsIgnoreCase("select")) return false
    if (code.contains("limit")) code = code.substring(code.lastIndexOf("limit")).trim
    else if (code.contains("LIMIT")) code = code.substring(code.lastIndexOf("LIMIT")).trim.toLowerCase
    else return true
    val hasLimit = code.matches("limit\\s+\\d+\\s*;?")
    if (hasLimit) {
      if (code.indexOf(";") > 0) code = code.substring(5, code.length - 1).trim
      else code = code.substring(5).trim
      val limitNum = code.toInt
      if (limitNum > defaultLimit) throw new IllegalArgumentException("We at most allowed to limit " + defaultLimit + ", but your SQL has been over the max rows.")
    }
    !hasLimit
  }
}
