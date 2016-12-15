package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object ProgramType_uv_pv_vv extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("ProgramType_uv_pv_vv")
    ModuleClass.executor(ProgramType_uv_pv_vv,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //calculate log whose type is play
        val path_play = "/mbi/parquet/playview/" + p.startDate
        val df_play = sqlContext.read.load(path_play)
        val playRDD = df_play.select("date","contentType","userId").
          map(e => (getKeys(e.getString(0), e.getString(1)), e.getString(2))).
          persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()
        playRDD.unpersist()

        val path_detail = "/mbi/parquet/detail/" + p.startDate
        val detailRDD = sqlContext.read.load(path_detail).select("date", "contentType", "userId").
          map(e => (getKeys(e.getString(0), e.getString(1), "detail"), e.getString(2))).
          persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()
        detailRDD.unpersist()

        val path_live = "/mbi/parquet/live/" + p.startDate
        val liveRDD = sqlContext.read.load(path_live).select("date", "userId").
          map(e => (getKeys(e.getString(0), "live"), e.getString(1))).
          persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_live = liveRDD.distinct().countByKey()
        val accessNum_live = liveRDD.countByKey()
        liveRDD.unpersist()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from programType_uv_pv_vv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO programType_uv_pv_vv(year,month,day,type,channel,uv_num,num) VALUES(?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })

        userNum_live.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum_live(x._1).toInt))
        })
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, contentType:String, logType:String = "play")={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    (year,month,date,logType,contentType)
  }

}
