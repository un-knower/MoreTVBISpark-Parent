package com.moretv.bi.overview

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
object OtherWatchMap extends BaseClass with DateUtil{

  def main(args: Array[String]) {
    config.setAppName("OtherWatchMap")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/interview/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event = 'enter' and path like '%otherwatch_map%'").select("date","path","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
                        map(e=>(getKeys(e._1,e._2),e._3)).filter(e=>e._1!=null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from otherwatch_map where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO otherwatch_map(year,month,day,region_code,region_name,user_num,access_num) VALUES(?,?,?,?,?,?,?)"
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

  def getKeys(date:String, path:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    val regionCode = if(path.contains("otherwatch_nearby")) "nearby" else "map"
    val regionName = if(path.contains("otherwatch_nearby")) "我的附近在看" else "大家都在看地图 "

    (year,month,date,regionCode,regionName)
  }
}
