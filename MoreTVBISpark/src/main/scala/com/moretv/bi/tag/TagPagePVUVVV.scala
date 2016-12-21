package com.moretv.bi.tag

import java.net.URLDecoder
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
object TagPagePVUVVV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("TagPagePVUVVV")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        //calculate log whose type is play
        val path = "/mbi/parquet/{playview,detail}/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df.filter("logType='playview' and event ='playview'").select("date","path","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
          filter(e =>judgePath(e._2)).map(e=>(getKeys(e._1,e._2),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val detailRDD = df.filter("logType='detail'").select("date","path","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
          filter(e =>judgePath(e._2)).map(e=>(getKeys(e._1,e._2,"detail"),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from tagPagePVUVVV where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO tagPagePVUVVV(year,month,day,type,tagname,user_num,access_num) VALUES(?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
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
    val reg = "home-\\w+-(tag-)+(.+?)".r
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

    //obtain module
    val decodePath = URLDecoder.decode(path,"UTF-8")
    val array = decodePath.split("-")
    val tag = array(array.length-1)
    (year,month,date,logType,tag)
  }
}
