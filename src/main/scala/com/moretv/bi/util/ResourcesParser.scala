package com.moretv.bi.util

import java.io.FileInputStream

import org.apache.logging.log4j.LogManager

import scala.io.Source

/**
  * Created by Administrator on 2017/1/13.
  */
object ResourcesParser {
  private val logger = LogManager.getLogger(this.getClass)

  /**
    * 用于获取Map型信息
    * @param filePath
    * @return
    */
  def getMapConf(filePath:String) = {
    val in = new FileInputStream(filePath)
    val lines = Source.fromInputStream(in).getLines()
    var lineNum = 0
    lines.map(line => {
      lineNum += 1
      if(validate(line)){
        val Array(key,value) = line.toString.split("=")
        (key.trim, value.trim)
      }else {
        logger.info("invalid conf line [{}] as {}",lineNum,line)
        null
      }
    }).filter(_!=null).toMap
  }

  /**
    * 用于获取List型信息
    */
  def getListConf(filePath:String) = {
    val in = new FileInputStream(filePath)
    val lines = Source.fromInputStream(in).getLines()
    var lineNum = 0
    lines.map(line => {
      lineNum += 1
        line.toString
    }).filter(_!=null).toList
  }


  /**
    * 对配置文件的每一行进行校验
    * 有效行必须包含以下条件
    * 1.不能以"#"开头
    * 2.必须包含"="(顺带也排除了空行)
    * 3.不能以"="结尾
    * @param line 配置文件中的行
    * @return 校验成功或失败的Boolean值
    */
  def validate(line:String) = {
    !line.startsWith("#") && line.contains("=") && !line.endsWith("=")
  }


  /**
    * Getting resources file from classload
    */


}
