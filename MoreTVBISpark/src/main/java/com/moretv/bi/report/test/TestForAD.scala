package com.moretv.bi.report.test

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object TestForAD extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(TestForAD,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val adDir = "/log/whaley/rawlog"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val rdd = sc.textFile(s"$adDir/$date/*/").filter(_.contains("ad-vod-pre-play")).
            filter(_.contains("start"))
          var i=0
          rdd.collect.foreach(e=>{
            println(e)
            i=i+1
            println(s"===================$i======================")
          })
          println(rdd.collect.length)
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
