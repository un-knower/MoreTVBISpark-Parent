package com.moretv.bi.set

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
object OptimizationToolsUsage extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("OptimizationToolsUsage")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.PAGEVIEW)
        val resultRDD = df.filter("page in ('network_check','network_source','network_speed')")
          .select("date","page","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
            map(e=>(getKeys(e._1,e._2),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from OptimizationToolsUsage where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO OptimizationToolsUsage(year,month,day,toolCode,toolName,uv_num,vv_num) VALUES(?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, page:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    val toolName = if(page == "network_check"){
                      "网络诊断"
                   }else if(page == "network_source"){
                      "视频源优化"
                   }else{
                      "网络测速"
                   }

    (year,month,date,page,toolName)
  }
}
