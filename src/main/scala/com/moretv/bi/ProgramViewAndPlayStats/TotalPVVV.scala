package com.moretv.bi.ProgramViewAndPlayStats

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
@deprecated
object TotalPVVV extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("TotalPVVV")
    ModuleClass.executor(TotalPVVV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //TODO 是否需要修改路径
        //calculate log whose type is play
        val playPath = "/mbi/parquet/playview/" + p.startDate
        val df_play = sqlContext.read.load(playPath)
        val playRDD = df_play.select("date","userId").
            map(e => (getKeys(e.getString(0)), e.getString(1))).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()
        playRDD.unpersist()

        val detailPath = "/mbi/parquet/detail/" + p.startDate
        val df_detail = sqlContext.read.load(detailPath)
        val detailRDD = df_detail.select("date","userId").
            map(e => (getKeys(e.getString(0),"detail"), e.getString(1))).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()
        detailRDD.unpersist()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from total_pv_vv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO total_pv_vv(year,month,day,weekstart_end,type,exit_type,user_num,access_num) VALUES(?,?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })
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

    val exit_type = if(logType == "play") "playview" else ""

    (year,month,date,week,logType,exit_type)
  }
}
