package com.moretv.bi.sports

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.sports.SportSubjectUVVV._
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
object SportZhuanTiUVVV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("SportZhuanTiUVVV")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.PLAYVIEW)
        val resultRDD = df.filter("path like 'home-sports-arragne_sport_news%'").select("date","path","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
            map(e=>(getKeys(e._1,e._2),e._3)).filter(e =>e._1._5 !=null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from sports_zhuanti_uv_vv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO sports_zhuanti_uv_vv(year,month,day,weekstart_end,subject_code,subject_name,uv_num,vv_num) VALUES(?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,CodeToNameUtils.getSubjectNameBySid(x._1._5),new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
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
    val week = getWeekStartToEnd(date)

    var subjectCode:String = null
    var subjectName:String = null

    val reg = "home-sports-arragne_sport_news-\\-?(sports\\d+)".r
    val pattern = reg findFirstMatchIn path
    pattern match {
      case Some(x) =>
        subjectCode = x.group(1)
      case None => null
    }

    (year,month,date,week,subjectCode)
  }
}
