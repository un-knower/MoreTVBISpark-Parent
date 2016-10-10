package com.moretv.bi.overview

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object HomeAccessProgramPVUVVV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("HomeAccessProgramPVUVVV")
    ModuleClass.executor(HomeAccessProgramPVUVVV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //calculate log whose type is play
        val pathPlay = "/mbi/parquet/playview/" + p.startDate
        val dfPlay = sqlContext.read.load(pathPlay)
        val playRDD = dfPlay.filter("path like '%hotrecommend%'").
          select("date", "videoSid", "contentType", "userId").
          map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3))).
          map(e => (getKeys(e._1, e._2, e._3), e._4)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val pathDetail = "/mbi/parquet/detail/" + p.startDate
        val dfDetail = sqlContext.read.load(pathDetail)
        val detailRDD = dfDetail.filter("path like '%hotrecommend%'").
          select("date", "videoSid", "contentType", "userId").
          map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3))).
          map(e => (getKeys(e._1, e._2, e._3, "detail"), e._4)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from homeAccessProgramPVUVVV where day = '$date'"
          util.delete(oldSql)
        }

        val redisUtil = new ProgramRedisUtils()
        //insert new data
        val sql = "INSERT INTO homeAccessProgramPVUVVV(year,month,day,type,contentType,title,sid,user_num,access_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,getTitleBySid(redisUtil,x._1._6),x._1._6,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,getTitleBySid(redisUtil,x._1._6),x._1._6,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })

        playRDD.unpersist()
        detailRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, videoSid:String, contentType:String, logType:String = "play")={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    (year,month,date,logType,contentType,videoSid)
  }

  def getTitleBySid(util:ProgramRedisUtils,sid:String)={
    var title =""
    title = util.getTitleBySid(sid)
    if(title == "" || title == null)
      title = sid
    else if(title != null){
      title = title.replace("'", "")
      title = title.replace("\t", " ")
      title = title.replace("\r", "-")
      title = title.replace("\n", "-")
      title = title.replace("\r\n", "-")
    }
    title
  }
}
