package com.moretv.bi.VideoPlayErrorAnalyze

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by laishun on 15/10/9.
 */
object ProgramPlayErrorStatistics extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("ProgramPlayErrorStatistics")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.PLAYQOS,p.startDate)
        val resultRDD = df.filter("event in('playerror','userexit','playend','sourceerror')").select("date","source","videoSid","event").map(e =>(e.getString(0),e.getString(1),e.getString(2),e.getString(3))).
            map(e=>(getKeys(e._1,e._2,e._3),e._4)).groupByKey().map(e => (e._1,countNum(e._2))).collect()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from program_play_error_statistics where day = '$date'"
          util.delete(oldSql)
        }
        val redisUtil = new ProgramRedisUtils()
        //insert new data
        val sql = "INSERT INTO program_play_error_statistics(year,month,day,source,sid,title,valid,playerror,sourceerror,total) VALUES(?,?,?,?,?,?,?,?,?,?)"
        resultRDD.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,getTitle(redisUtil,x._1._5),new Integer(x._2._1),new Integer(x._2._2),new Integer(x._2._3),new Integer(x._2._4))
        })
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
  
  def getKeys(date:String, source:String, videoSid:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    (year,month,date,source,videoSid)
  }

  def getTitle(util:ProgramRedisUtils,sid:String)={
    var title = util.getTitleBySid(sid)
    if(title != null){
      title = title.replace("'", "");
      title = title.replace("\t", " ");
      title = title.replace("\r", "-");
      title = title.replace("\n", "-");
      title = title.replace("\r\n", "-");
    }else title = sid
    title
  }

  def countNum(iter: Iterable[String]) ={
    var success = 0;
    var playerror = 0;
    var sourceerror = 0;
    iter.foreach(e =>{
      e match {
        case "playerror" => playerror =playerror+1
        case "sourceerror" => sourceerror =sourceerror+1
        case "userexit" => success =success+1
        case "playend" => success =success+1
      }
    })
    (success,playerror,sourceerror,success+playerror+sourceerror)
  }
}
