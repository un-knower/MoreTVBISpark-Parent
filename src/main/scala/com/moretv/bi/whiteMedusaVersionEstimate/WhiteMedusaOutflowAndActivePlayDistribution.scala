package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}


/**
  * 统计白猫版本的用户在T+5内均活跃以及未活跃的用户在3月份的播放情况对比
  */

object WhiteMedusaOutflowAndActivePlayDistribution extends BaseClass {

  private val insertSql = "insert into white_medusa_user_play_distribution(period,user_type,content_type,play_num,play_user) values (?,?,?,?,?)"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse("20170301"))
        val dateArr = new scala.collection.mutable.ArrayBuffer[String]()
        (0 until 31).foreach(j=>{
          calendar.add(Calendar.DAY_OF_MONTH,1)
          dateArr.+=(DateFormatUtils.readFormat.format(calendar.getTime))
        })
        DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA,LogTypes.PLAY,dateArr.toArray).
          filter("event = 'startplay'").select("userId","contentType").registerTempTable("play_log")

        val userIdDir = s"/log/medusa/temple/userId/201704{26,27,28,29}/"
        sqlContext.read.load(userIdDir).distinct().registerTempTable("user_log")

        val playTypeDistribution = sqlContext.sql(
          """
            |select a.userType,b.contentType,count(a.userId),count(distinct a.userId)
            |from user_log as a
            |join play_log as b
            |on a.userId = b.userId
            |group by a.userType,b.contentType
          """.stripMargin).map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3))).collect()

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        playTypeDistribution.foreach(e=>{
          util.insert(insertSql,"March",e._1,e._2,e._3,e._4)
        })
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

}