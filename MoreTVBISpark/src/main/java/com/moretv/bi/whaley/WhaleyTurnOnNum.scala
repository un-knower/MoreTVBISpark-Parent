package com.moretv.bi.whaley

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, SparkSetting, UserIdUtils}
import org.apache.spark.SparkContext
import org.json.JSONObject

/**
 * Created by Will on 2015/4/18.
 */
object WhaleyTurnOnNum extends BaseClass{
  def main(args: Array[String]) {
    config.setAppName("WhaleyTurnOnNum")
    ModuleClass.executor(WhaleyTurnOnNum,args)
  }
  override def execute(args: Array[String]) {
    val cacheValue = sc.textFile(args(0)).map(new JSONObject(_)).filter(json =>(json.optString("userId")!=null && json.optString("logType")=="on")).
      map(json => (json.optString("date"),json.optString("userId"))).persist()
    /** 计算人数和次数*/
    val userNumValue = cacheValue.distinct().countByKey()
    val accessNumValue = cacheValue.countByKey()
    val file = "/home/moretv/mbi/test/whaleyTurnOnTotalNum.txt"
    val out = new PrintWriter(file,"GBK")
    userNumValue.foreach(
      x =>{
        printf(x._1+"\t"+x._2+"\t"+accessNumValue.get(x._1).get)
        out.println(x._1+","+x._2+","+accessNumValue.get(x._1).get)
      }
    )
    cacheValue.unpersist()
  }
}
