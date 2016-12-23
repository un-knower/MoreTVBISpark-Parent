package com.moretv.bi.UserTrend

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object User_use_duration extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("User_use_duration")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.ENTER,p.startDate)
        val resultRDD = df.select("datetime","userId").map(e =>(e.getString(0),e.getString(1))).
            map(e=>(getKeys(e._1),e._2)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from User_use_duration where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO User_use_duration(year,month,day,hour,uv,vv) VALUES(?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
  
  def getKeys(date:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val currentTime = date.substring(0,11)
    val hour = date.substring(11,13)
    (year,month,currentTime,hour)
  }
}
