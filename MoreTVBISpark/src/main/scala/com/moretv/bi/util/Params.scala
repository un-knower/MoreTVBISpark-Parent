package com.moretv.bi.util

/**
 * Created by laishun on 15/10/12.
 */

case class Params (startDate:String ="yyyyMMdd", //起始日期,格式为yyyyMMdd
                   endDate: String = "yyyyMMdd", //结束日期,格式为yyyyMMdd
                   whichMonth:String = "", //执行的月份,格式为yyyyMM
                   deleteOld:Boolean=false, //是否删除旧数据
                   logType:String = "", //单个日志类型
                   columns:String = "", //多个列名，以英文逗号(,)分割
                   fileDir:String = "", // 数据所在的目录
//                   applicationMode:String = "medusa", // 区分哪一个应用
                   logTypes:String = "", //多个日志类型，以英文逗号(,)分割
                   srcPath: String = "", // 数据源路径
//                   statXml: String = "", //用于做统计xml文件
//                   database:String = "", //插入数据到哪个数据库
//                   startTime:String = "HH:mm:ss", //开始时间，格式为HH:mm:ss
//                   endTime:String = "HH:mm:ss", //结束时间，格式为HH:mm:ss
//                   sid:String = "", //节目的sid
                   whichDay:Int = 1, //
                   numOfDays:Int = 1, //天数
                   offset:Int = 1,//偏移量
                   apkVersion:String = "",
                   durationMax:Int  = 0, //播放时长区间最大值
                   alarmFlag:Boolean = true,  //限定版本号
//                   contentType:String = "",
//                   dateInfo:String="" ,
                   outputFile: String = "", //输出csv文件路径
                   paramMap: Map[String,String] = Map[String,String]() //参数集合
                  ){

  def get[T](key:String) = paramMap(key).asInstanceOf[T]

  def getOrElse[T](key:String,default: => T) = {
    paramMap.get(key) match {
      case Some(value) => value.asInstanceOf[T]
      case None => default
    }
  }

  def getString = get[String] _
  def getInt = get[Int] _
  def getLong = get[Long] _
  def getDouble = get[Double] _
  def getBoolean = get[Boolean] _
  def contains = paramMap.contains _

}


