package com.moretv.bi.kidslogin

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/2/16.
  */
object MtvKidsAccessUsers extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(MtvKidsAccessUsers,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val inputPath = s"/log/mtvkidsloginlog/parquet/$inputDate/loginlog"

        val logRdd = sqlContext.read.load(inputPath).select("mac").cache()
        val loginNum = logRdd.count()
        val userNum = logRdd.distinct().count()

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld){
          val sqlDelete = "delete from mtv_kids_useraccess where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into mtv_kids_useraccess(day,user_num,access_num) values(?,?,?)"

        db.insert(sqlInsert,day,new Integer(userNum.toInt),new Integer(loginNum.toInt))
        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
