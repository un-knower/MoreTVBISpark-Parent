package com.moretv.bi.tag

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
object AddTagAndCommentOperation extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("AddTagAndCommentOperation")
    ModuleClass.executor(AddTagAndCommentOperation,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/operation-acw/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event in ('addtag','comment')").select("date","event","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
                           map(e=>(getKeys(e._1,e._2),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from addTagAndCommentOperation where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO addTagAndCommentOperation(year,month,day,type,user_num,access_num) VALUES(?,?,?,?,?,?)"
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

  def getKeys(date:String, event:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    (year,month,date,event)
  }
}
