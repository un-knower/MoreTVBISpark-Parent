package com.moretv.bi.report.medusa.tempAnalysis

import java.lang.{Long => JLong}
import java.util
import java.util.Calendar

import cn.whaley.bi.utils.{ElasticSearchUtil, HttpUtils}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Constants
import com.moretv.bi.global.LogTypes
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xiajun on 2016/5/16.
  *
  */
object Insert2ES extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {



          val videoList = new util.ArrayList[util.Map[String, Object]]()



          (0 until 10).foreach(e=>{
            val resMap = new util.HashMap[String,Object]()
            resMap.put("contentType","movie")
            resMap.put("episodeSid", "dhfsfieuhrihf")
            resMap.put("title","神盾局特工")
            resMap.put("day", "2000-01-01")
            resMap.put("userNum", "374")
            resMap.put("accessNum", "39475")
            videoList.add(resMap)
          })
          ElasticSearchUtil.bulkCreateIndex(videoList, "medusa", "programPlay")

        })
        ElasticSearchUtil.close
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
