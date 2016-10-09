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
object HotRecommendSanjiPagePVUVVV extends BaseClass with DateUtil{

  def main(args: Array[String]) {
    config.setAppName("HotRecommendSanjiPagePVUVVV")
    ModuleClass.executor(HotRecommendSanjiPagePVUVVV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //calculate log whose type is play
        val path = "/mbi/parquet/{playview,detail}/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df.filter("logType='playview'").select("date","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            filter(e =>judgePath(e._2)).map(e => (getKeys(e._1, e._2), e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val detailRDD = df.filter("logType='detail'").select("date", "path", "userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            filter(e =>judgePath(e._2)).map(e => (getKeys(e._1, e._2, "detail"), e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from hotrecommendSanjiPagePVUVVV where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO hotrecommendSanjiPagePVUVVV(year,month,day,type,secondPath,thirdPathCode,thirdpathName,user_num,access_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,CodeToNameUtils.getThirdPathName(x._1._6),new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,CodeToNameUtils.getThirdPathName(x._1._6),new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })

        playRDD.unpersist()
        detailRDD.unpersist()
        df.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def judgePath(path:String) = {
    val reg = "home-hotrecommend(-\\d+-\\d+)?-(movie|tv|zongyi|kids|comic|mv|jilu|xiqu|hot|sports)-(\\w+)".r
    val pattern = reg findFirstMatchIn path
    val res = pattern match {
      case Some(x) => true
      case None => false
    }
    res
  }

  def getKeys(date:String, path:String, logType:String = "play")={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    val array = path.split("-")
    val secondPath = array(array.length-2)
    val thirdPath = array(array.length-1)

    (year,month,date,logType,secondPath,thirdPath)
  }
}
