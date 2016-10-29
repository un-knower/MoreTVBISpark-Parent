package com.moretv.bi.temp

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/28/16.
  */
object GetOutOfMyWay extends BaseClass {

  private val startDate = "2016-07-01"
  private val endDate = "2016-09-30"

  private val playEvent = "'play','playview'"
  private val playStart = "2016-07-01"
  private val playEnd = "2016-09-30"

  private val playDurationEvent = "'userexit','selfend'"
  private val playDurationStart = "2016-07-01"
  private val playDurationEnd = "2016-09-30"

  private val searchEvent = "'play','playview','detail'"
  private val searchStart = "2016-07-01"
  private val searchEnd = "2016-09-30"

  private val exitEvent = "'exit'"
  private val exitStart = "2016-07-01"
  private val exitEnd = "2016-09-30"

  private val enterEvent = "'enter'"
  private val timeEnums = "'2016-09-01','2016-10-01'"
  private val timeFlag = "continuous"
  private val logType = "'',''"


  private val sqlIntervalFormat =
    s"""
       |select count(userId) as pv , count(distinct userId) as uv
       | from log_data
       | where date in ($timeEnums)
       | and logType in ()
       | and event in ()
    """.stripMargin

  private val sqlContinuousFormat =
    s"""
       |select count(userId) as pv , count(distinct userId) as uv
       | from log_data
       | where date between '$startDate' and '$endDate'
       | and logType in ()
       | and event in ()
    """.stripMargin

  def main(args: Array[String]) {

    ModuleClass.executor(GetOutOfMyWay, args)

  }

  /**
    * 季度搜索总量，
    * 季度视频播放总量，
    * 播放时长
    * 单活跃用户搜索量
    * 单活跃用户视频播放量
    * 单活跃用户停留时间
    * 单活跃用户播放时长
    *
    * @param args
    */
  override def execute(args: Array[String]): Unit = {

    val loadAllPath = "/log/medusaAndMoretvMerger/2016{07*,08*,09*}/*"

    val loadSubjectPath = "/log/medusaAndMoretvMerger/2016{07*,08*,09*}/{playview,detail,exit,enter}"

    //季活
    val userNum = sqlContext.read.parquet(loadAllPath)
      .filter(s"date between '$start' and '$end'")
      .select("userId")
      .distinct
      .count

    println(userNum)

    //////////////////////////

    sqlContext.read.parquet(loadSubjectPath)
      .filter(s"date between '$playStart' and '2016-09-30'")
      .registerTempTable("log_data")


    //季度搜索人数
    val searchNum = sqlContext.sql(
      s"""
         |select userId from log_data
         | where (path like '%search%' or pathMain like '%search%')
         | and event in ($searchEvent)
      """.stripMargin)
      .count

    println(searchNum)
    println(searchNum.toFloat / userNum)

    //季度播放人数
    val playNum = sqlContext.sql(
      s"""
         |select userId from log_data
         | where event in ($playEvent)
      """.stripMargin)
      .count

    println(playNum)
    println(playNum.toFloat / userNum)

    //季度播放时长
    sqlContext.sql(
      s"""
         |select * from log_data
         | where event in ($playDurationEvent)
      """.stripMargin)
      .registerTempTable("log_data_play")

    val duration = sqlContext.sql("select duration from log_data ")
      .map(e => e.getLong(0))
      .sum()

    println(duration)
    println(duration.toFloat / userNum)


  }


  def parseCmd(logType:String, events: Array[String],)


}
