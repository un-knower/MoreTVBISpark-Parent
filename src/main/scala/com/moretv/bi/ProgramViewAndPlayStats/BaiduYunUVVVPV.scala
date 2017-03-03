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
object BaiduYunUVVVPV extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("BaiduYunUVVVPV")
    ModuleClass.executor(BaiduYunUVVVPV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {


        //calculate log whose type is play
        //TODO 是否需要修改路径
        val path = "/mbi/parquet/{playview,interview}/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df.filter("logType='playview' and path like '%home-baidu_cloud%'").select("date","userId").map(e => (e.getString(0), e.getString(1))).
            map(e => (getKeys(e._1), e._2)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val detailRDD = df.filter("logType='interview' and path like '%home-baidu_cloud%'").select("date", "userId").map(e => (e.getString(0), e.getString(1))).
            map(e => (getKeys(e._1, "interview"), e._2)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_interview = detailRDD.distinct().countByKey()
        val accessNum_interview = detailRDD.countByKey()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from commonUVVVPV where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO commonUVVVPV(year,month,day,weekstart_end,object,logType,uv_num,vv_num) VALUES(?,?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_interview.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2.toInt),new Integer(accessNum_interview(x._1).toInt))
        })

        playRDD.unpersist()
        detailRDD.unpersist()
        df.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, logType:String = "play")={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    (year,month,date,week,"baidu_cloud",logType)
  }
}
