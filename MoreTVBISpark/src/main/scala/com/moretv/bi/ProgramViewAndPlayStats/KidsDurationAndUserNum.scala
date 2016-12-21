package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.set.WallpaperSetUsage._
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
object KidsDurationAndUserNum extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("KidsDurationAndUserNum")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.INTERVIEW)
        val resultRDD = df.filter("event = 'exit' and path in ('home-kids_home','thirdparty_1-kids_home'," +
          "'thirdparty_0-kids_home')").select("date","productModel","duration","userId").map(e =>(e.getString(0),e
          .getString(1),e.getInt(2),e.getString(3))).
            map(e=>(getKeys(e._1,e._2),e._3,e._4)).persist(StorageLevel.MEMORY_AND_DISK)

        val userNum = resultRDD.map(e =>(e._1,e._3)).distinct().countByKey()
        val accessNum = resultRDD.map(e =>(e._1,e._3)).countByKey()
        val duration = resultRDD.map(e =>(e._1,e._2)).filter(_._2 < 72000).reduceByKey((x,y)=>x+y).collect().toMap

        val util = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from kids_duration_user_num where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO kids_duration_user_num(year,month,day,weekstart_end,product_model,user_num,total_time,access_num) VALUES(?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          try{
            util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(duration(x._1).toInt),new Integer(accessNum(x._1).toInt))
          }catch {
            case e:Exception =>
          }
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, productModel:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    (year,month,date,week,productModel)
  }
}
