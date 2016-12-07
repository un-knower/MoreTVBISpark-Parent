package com.moretv.bi.util

/**
  * Created by laishun on 15/10/12.
  */

case class Params(startDate: String = "yyyyMMdd", //起始日期,格式为yyyyMMdd
                  endDate: String = "yyyyMMdd", //结束日期,格式为yyyyMMdd
                  startTime: String = "HH:mm:ss", //开始时间，格式为HH:mm:ss
                  endTime: String = "HH:mm:ss", //结束时间，格式为HH:mm:ss
                  whichMonth: String = "", //执行的月份,格式为yyyyMM
                  numOfDays: Int = 1, //天数
                  deleteOld: Boolean = false, //是否删除旧数据
                  logType: String = "", //单个日志类型
                  srcPath: String = "", // 数据源路径
                  statXml: String = "", //用于做统计xml文件
                  applicationMode: String = "medusa", // 区分哪一个应用
                  logTypes: String = "", //多个日志类型，以英文逗号(,)分割
                  alarmFlag: Boolean = true, //限定版本号
                  outputFile: String = "" //输出csv文件路径
                 )

