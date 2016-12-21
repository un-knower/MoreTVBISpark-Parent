package com.moretv.bi.VideoPlayErrorAnalyze

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by laishun on 15/10/9.
 */
object PlaySourceErrorStatisitcs extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("PlaySourceErrorStatisitcs")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.PLAYQOS,p.startDate)
        val resultRDD = df.filter("event in('userexit','playend','sourceerror')").select("date","apkSeries","source","contentType","event").map(e =>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4))).
            map(e=>(getKeys(e._1,e._2,e._3,e._4),e._5)).groupByKey().map(e => (e._1,countNum(e._2))).collect()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from play_source_error where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO play_source_error(year,month,day,version,source,type,success,error,total) VALUES(?,?,?,?,?,?,?,?,?)"
        resultRDD.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2._1),new Integer(x._2._2),new Integer(x._2._3))
        })
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
  
  def getKeys(date:String, apkSeries:String, source:String, contentType:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)+1

    (year,month,date,apkSeries,source,contentType)
  }

  def countNum(iter: Iterable[String]) ={
    var success = 0;
    var faile = 0;
    iter.foreach(e =>{
      e match {
        case "sourceerror" => faile =faile+1
        case "userexit" => success =success+1
        case "playend" => success =success+1
      }
    })
    (success,faile,success+faile)
  }
}
