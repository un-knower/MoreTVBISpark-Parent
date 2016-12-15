package com.moretv.bi.apprecommend

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
object AppRecommendUseNum extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("AppRecommendUseNum")
    ModuleClass.executor(AppRecommendUseNum,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/apprecommend/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.select("date","event","userId","appSid").map(e =>(e.getString(0),e.getString(1),e.getString(2),e.getString(3))).
          map(e=>(getKeys(e._1,e._2,e._4),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from app_recommend where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO app_recommend(year,month,day,weekstart_end,operation,sid,app_name,user_num,operation_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, event:String, appSid:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)+1
    val week = getWeekStartToEnd(date)

    (year,month,date,week,event,appSid,CodeToNameUtils.getApplicationNameBySid(appSid))
  }
}
