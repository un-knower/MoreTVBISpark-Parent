package com.moretv.bi.tag

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
object MoreTagOperation extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("MoreTagOperation")
    ModuleClass.executor(MoreTagOperation,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/operation-mm/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event='moretag'").select("date","userId").map(e =>(e.getString(0),e.getString(1))).
                           map(e=>(getKeys(e._1),e._2)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from moreTagOperation where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO moreTagOperation(year,month,day,type,user_num,access_num) VALUES(?,?,?,?,?,?)"
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

    (year,month,date,"moretag")
  }
}
