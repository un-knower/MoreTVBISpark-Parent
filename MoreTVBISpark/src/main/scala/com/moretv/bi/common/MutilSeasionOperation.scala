package com.moretv.bi.common

import java.text.SimpleDateFormat
import java.util.{Locale, Calendar}

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/4/18.
 */
object MutilSeasionOperation extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("MutilSeasion")
    ModuleClass.executor(MutilSeasionOperation,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val path = "/mbi/parquet/operation-mm/"+p.startDate+"/part-*"
        val cacheValue = sqlContext.read.load(path).filter("event='multiseason'").select("date","userId").map(e=>(e.getString(0),e.getString(1))).persist()
        /** 计算人数和次数*/
        val userNumValue = cacheValue.distinct().countByKey()
        val accessNumValue = cacheValue.countByKey()
        var sql = ""
        val dbUtil = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from mutilseasionoperation where day = '$date'"
          dbUtil.delete(oldSql)
        }
        userNumValue.foreach(
          x =>{
            sql = "insert into mutilseasionoperation(day,user_num,access_num) values(?,?,?)"
            dbUtil.insert(sql,x._1,new Integer(x._2.toInt),new Integer(accessNumValue.get(x._1).get.toInt))
          }
        )
        dbUtil.destory()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
