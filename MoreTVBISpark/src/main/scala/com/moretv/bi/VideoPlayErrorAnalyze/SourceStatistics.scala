package com.moretv.bi.VideoPlayErrorAnalyze

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by laishun on 15/10/9.
 */
object SourceStatistics extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("SourceStatistics")
    ModuleClass.executor(SourceStatistics,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val path = "/mbi/parquet/playqos/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event in('playerror','userexit','playend','sourceerror')").select("date","source","event").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
            map(e=>(getKeys(e._1,e._2),e._3)).groupByKey().map(e => (e._1,countNum(e._2))).collect()

        val util = new DBOperationUtils("bi")
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from source_statistics where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO source_statistics(year,month,day,source,valid,playerror,sourceerror,total) VALUES(?,?,?,?,?,?,?,?)"
        resultRDD.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,new Integer(x._2._1),new Integer(x._2._2),new Integer(x._2._3),new Integer(x._2._4))
        })
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
  
  def getKeys(date:String, source:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    (year,month,date,source)
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
