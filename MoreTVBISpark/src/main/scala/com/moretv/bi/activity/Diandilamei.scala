package com.moretv.bi.activity

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.moretv.bi.util._
import java.util.Calendar

/**
  * Created by Huzhehua on 2016/4/18.
  */

/** 垫底辣妹活动首页专题推荐统计
  * 统计访问专题的人数次数以及播放预告片的人数次数
  * */
object Diandilamei extends BaseClass {
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //val cal = Calendar.getInstance()
        //cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          //  val date = DateFormatUtils.readFormat.format(cal.getTime)
          val pageviewRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, ACTIVITY, LogTypes.PAGEVIEW)
            .filter("activityId = 'i67oe523m7p8'")
            .select("userId").cache()
          val watchvideoRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, ACTIVITY, LogTypes.OPERATION)
            .filter("activityId = 'i67oe523m7p8' and event = 'watchvideo'")
            .select("userId").cache()
          val access_num = pageviewRdd.count()
          val access_user_num = pageviewRdd.distinct().count()
          val play_num = watchvideoRdd.count()
          val play_user_num = watchvideoRdd.distinct().count()
          //  println("**********"+date+"*********")
          println("access_num : " + access_num)
          println("access_user_num : " + access_user_num)
          println("play_num : " + play_num)
          println("play_user_num : " + play_user_num)
          // cal.add(Calendar.DAY_OF_MONTH,-1)
        })
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }
}
