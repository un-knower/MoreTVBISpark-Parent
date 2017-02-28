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
object KidsSongUVVV extends BaseClass with DateUtil{

  def main(args: Array[String]): Unit = {
    config.setAppName("KidsSongUVVV")
    ModuleClass.executor(KidsSongUVVV,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/playview/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("path like '%kids_home-kids_songhome%'").select("date","videoSid","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
            map(e=>(getKeys(e._1,e._2),e._3)).persist(StorageLevel.MEMORY_AND_DISK)

        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = new DBOperationUtils("eagletv")
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from kids_song_uv_vv where day = '$date'"
          util.delete(oldSql)
        }
        val redisUtil = new ProgramRedisUtils()
        //insert new data
        val sql = "INSERT INTO kids_song_uv_vv(year,month,day,weekstart_end,exit_type,sid,song_name,user_num,play_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,getTitle(redisUtil,x._1._6),new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, videoSid:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)
    (year,month,date,week,"playview",videoSid)
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