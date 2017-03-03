package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by Administrator on 2016/12/27
 *
 */

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject

object CrashToParquet extends SparkSetting{
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        val outPath = "/log/medusa_crash/parquet/"
        // 过滤掉stack_trace没有值/空的情形
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          if(p.deleteOld){
            HdfsUtil.deleteHDFSFile(s"${outPath}${date}/")
          }
          // TODO 是否需要写到固定的常量类或者SDK读取
          val logRdd = sc.textFile(s"/log/medusa_crash/rawlog/${date}/").map(log=>{
            var json = new JSONObject()
            try{
              json = new JSONObject(log)
            }catch{
              case e:Exception=>{println(log)}
            }
            (json.optString("fileName"),json.optString("MAC").replace(":","").toUpperCase,
              json.optString("APP_VERSION_NAME"),
              json.optString("APP_VERSION_CODE"),json.optString("CRASH_KEY"),
              json.optString("STACK_TRACE"),json.optString("DATE_CODE"),
              json.optString("PRODUCT_CODE"),json.optString("CUSTOM_JSON_DATA"))
          }).filter(e=>{e._6!=null && e._6!=""  && {if(e._7!=null) e._7.length<=20 else true}})

          // 所有的crash
          val logDF = logRdd.map(log => (insertDate,log._1,log._2,log._3.replace(":","").toUpperCase,log._4,log._5,log._6,log._7,log
            ._8,DigestUtils.md5Hex(log._5),DigestUtils.md5Hex(log._6),log._9)).
            filter(data => !DevMacUtils.macFilter(data._2)).toDF("dayInfo","fileName","mac","appVersionName",
              "appVersionCode","crashKey","stackTrace","dateCode","productModel","crashKeyMD5",
              "stackTraceMD5","customJsonData")
          logDF.write.parquet(s"${outPath}${date}/")
        })
      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}
