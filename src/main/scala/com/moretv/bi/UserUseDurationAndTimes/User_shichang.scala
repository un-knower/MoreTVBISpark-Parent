package com.moretv.bi.UserUseDurationAndTimes

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object User_shichang extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("User_shichang")
    ModuleClass.executor(User_shichang,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        //TODO 是否需要写到固定的常量类or通过SDK读取
        val path = "/mbi/parquet/exit/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.select("date","duration","userId").map(e =>(e.getString(0),e.getInt(1),e.getString(2))).
            map(e=>(getKeys(e._1,e._2),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = new DBOperationUtils("eagletv")
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from User_use_duration where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO user_shichang(day,period,user_num,count) VALUES(?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,x._1._1,x._1._2,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
  
  def getKeys(date:String, duration:Long)={

    var hour = ""
    if(duration <= 30){
      hour ="0-30s"
    }else if(duration > 30 && duration <= 60){
      hour ="30-60s"
    }else if(duration > 60 && duration <= 180){
      hour ="60-180s"
    }else if(duration > 180 && duration <= 300){
      hour ="180-300s"
    }else if(duration > 300 && duration <= 900){
      hour ="5-15min"
    }else if(duration > 900 && duration <= 1800){
      hour ="15-30min"
    }else if(duration > 1800 && duration <= 3600){
      hour ="30-60min"
    }else if(duration > 3600 && duration <= 7200){
      hour ="1-2h"
    }else if(duration > 7200 && duration <= 10800){
      hour ="2-3h"
    }else if(duration > 10800 && duration <= 14400){
      hour ="3-4h"
    }else if(duration > 14400 && duration <= 18000){
      hour ="4-5h"
    }else if(duration > 18000 && duration <= 21600){
      hour ="5-6h"
    }else if(duration > 21600){
      hour =">6h"
    }
    (date,hour)
  }
}
