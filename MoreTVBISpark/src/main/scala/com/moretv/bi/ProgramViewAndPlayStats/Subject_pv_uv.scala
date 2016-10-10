package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.constant.LogType._
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import com.moretv.bi.util.SubjectUtils._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

object Subject_pv_uv extends BaseClass with DateUtil{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(Subject_pv_uv,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val logType = DETAIL
        val sqlLog = "select path,userId from log_data"
        val logRdd = DFUtil.getDFByDateWithSql(sqlLog,logType,p.startDate).
          flatMap(row => getSubjectCodeAndPathWithId(row.getString(0),row.getString(1))).persist(StorageLevel.MEMORY_AND_DISK)
        val pvNums = logRdd.countByKey()
        val uvNums = logRdd.distinct().countByKey()
        //save date
        val util = new DBOperationUtils("eagletv")
        //delete old data
        val date = DateFormatUtils.toDateCN(p.startDate, -1)
        if (p.deleteOld) {
          val oldSql = s"delete from subject_pv_uv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO subject_pv_uv(year,month,day,weekstart_end,type,path,uv_num,pv_num) VALUES(?,?,?,?,?,?,?,?)"
        pvNums.foreach(x => {
          val uvNum = uvNums(x._1)
          val (year,month,week) = getKeys(date)
          util.insert(sql,new Integer(year),new Integer(month),date,week,x._1._1,x._1._2,new Integer(uvNum.toInt),new Integer(x._2.toInt))
        })
        util.destory()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String)={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    (year,month,week)
  }
}

