package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object Third_path_vv extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("Third_path_vv")
    ModuleClass.executor(Third_path_vv,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //TODO 是否需要修改路径
        //calculate log whose type is play
        val path = "/mbi/parquet/playview/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path)
        val allRDD = df.select("date","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            map(e => (getKeys(e._1,e._2,"All"), e._3)).filter(e =>(e._1._5!=null && e._1._6!=null)).persist(StorageLevel.MEMORY_AND_DISK)

        val kidsRDD = df.filter("path like '%home-kids_home-kids_seecartoon%'").select("date","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
          map(e => (getKeys(e._1,e._2,"kids"), e._3)).filter(e =>(e._1._5!=null && e._1._6!=null)).persist(StorageLevel.MEMORY_AND_DISK)

        val sportsRDD = df.filter("path like '%home-sports%'").select("date","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
          map(e => (getKeys(e._1,e._2,"sports"), e._3)).filter(e =>(e._1._5!=null && e._1._6!=null)).persist(StorageLevel.MEMORY_AND_DISK)


        val playRDD = allRDD.union(kidsRDD).union(sportsRDD).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = playRDD.distinct().countByKey()
        val accessNum = playRDD.countByKey()

        //save date
        val util = new DBOperationUtils("eagletv")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from third_path_vv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO third_path_vv(year,month,day,weekstart_end,channel,path,uv_num,vv_num) VALUES(?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        playRDD.unpersist()
        sportsRDD.unpersist()
        allRDD.unpersist()
        kidsRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, pathPar:String, flag:String)={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    var channel:String = null
    var path:String = null

    var reg = "home-(movie|tv|zongyi|comic|kids|jilu|mv|hot|xiqu|kids_home|sports)-(\\w+)".r
    if(flag == "kids")
      reg = "home-(kids_home)-kids_seecartoon-(\\w+)".r
    else if(flag == "sports")
      reg = "home-(sports)-\\w+-(\\w+)".r

    val pattern = reg findFirstMatchIn pathPar
    pattern match {
      case Some(x) =>
        channel = x.group(1)
        path = x.group(2)
      case None => null
    }

    (year,month,date,week,channel,path)
  }
}
