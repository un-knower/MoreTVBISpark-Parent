package com.moretv.bi.overview

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
object TotalUVVVStatistics extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("HotRecommendSanjiPagePVUVVV")
    ModuleClass.executor(TotalUVVVStatistics,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //calculate log whose type is play
        val path = "/mbi/parquet/{playview,live}/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df.filter("logType='playview'").select("date","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            filter(e =>judgePath(e._2,"play")).map(e => (getKeys(e._1), e._3))

        val detailRDD = df.filter("logType='live'").select("date", "path", "userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            filter(e =>judgePath(e._2,"live")).map(e => (getKeys(e._1), e._3)).union(playRDD).persist(StorageLevel.MEMORY_AND_DISK)

        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from total_uvvv_statistics where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO total_uvvv_statistics(year,month,day,user_num,access_num) VALUES(?,?,?,?,?)"
        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })

        detailRDD.unpersist()
        df.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def judgePath(path:String,flag:String) = {
    val reg = if(flag == "play")
                "(home|thirdparty_\\d{1})".r
              else
                "(home|thirdparty_\\d{1})-(live|TVlive)".r
    val pattern = reg findFirstMatchIn path
    val res = pattern match {
      case Some(x) => true
      case None => false
    }
    res
  }

  def getKeys(date:String)={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    (year,month,date)
  }
}
