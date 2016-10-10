package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object ProgramVV extends BaseClass with DateUtil{

  def main(args: Array[String]): Unit = {
    config.setAppName("ProgramVV")
    ModuleClass.executor(ProgramVV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //calculate log whose type is play
        val pathA = "/mbi/parquet/playview/" + p.startDate
        val pathB = "/mbi/parquet/playqos/" + p.startDate
        val resultA = sqlContext.read.load(pathA).select("date","contentType","videoSid").
          map(row => (row.getString(0),row.getString(1),row.getString(2),"playview")).
          countByValue()
        val resultB = sqlContext.read.load(pathB).select("date","contentType","videoSid","event","apkVersion").
          map(row => (row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4))).
          filter(x => PlayExitTypeUtils.isVVLog(x._5)).
          countByValue()

        sc.stop()

        //save date
        val util = new DBOperationUtils("eagletv")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from program_vv_data where day = '$date'"
          util.delete(oldSql)
        }
        val redisUtil = new ProgramRedisUtils()
        //insert new data
        val sql = "INSERT INTO program_vv_data(year,month,day,weekstart_end,path,exit_type,title,sid,vv_num) VALUES(?,?,?,?,?,?,?,?,?)"
        resultA.foreach(x => {
          val (year,month,day,week) = getKeys(x._1._1)
          val title = getTitle(redisUtil,x._1._3)
          util.insert(sql,year,month,day,week,x._1._2,"playview",title,x._1._3,new Integer(x._2.toInt))
        })
        resultB.foreach(x => {
          val (year,month,day,week) = getKeys(x._1._1)
          val title = getTitle(redisUtil,x._1._3)
          util.insert(sql,year,month,day,week,x._1._2,x._1._4,title,x._1._3,new Integer(x._2.toInt))
        })
        util.destory()
        redisUtil.destroy()

      }
      case None =>{
        throw new RuntimeException("At least need param --startDate.")
      }
    }

  }

  def getKeys(date:String)={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)
    (new Integer(year),new Integer(month),date,week)
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
}
