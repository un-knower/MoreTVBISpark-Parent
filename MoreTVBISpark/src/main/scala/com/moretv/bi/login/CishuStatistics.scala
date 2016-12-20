package com.moretv.bi.login

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/2/16.
  */
object CishuStatistics extends BaseClass{

  val regex = "^[\\w\\.]+$".r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(CishuStatistics,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"

        val logRdd = sqlContext.read.load(inputPath).select("version","userId").
          map(row => {
            val version = row.getString(0)
            val userId = row.getString(1)
            if(version == null)(("null",userId),1l) else ((version,userId),1l)
          })
        val loginNums = logRdd.reduceByKey(_+_)
        val result = loginNums.map(x => {
          val version = x._1._1
          val num = x._2
          val cishu = if (num <= 3) {
            s"登入${num}次"
          }else if (num <= 5) {
            "登入4-5次"
          }
          else if (num <= 8) {
            "登入6-8次"
          }
          else if (num <= 15) {
            "登入9-15次"
          }
          else {
            "登入大于15次"
          }
          (version,cishu)
        }).countByValue()

        val db = DataIO.getMySqlOps("moretv_eagletv_mysql")
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld){
          val sqlDelete = "delete from cishu_tongji where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into cishu_tongji(day,apk_version,cishu,user_num) values(?,?,?,?)"
        result.foreach(x => {
          val (version,cishu) = x._1
          regex findFirstMatchIn version match {
            case Some(v) => {
              val userNum = x._2
              db.insert(sqlInsert,day,version,cishu,userNum)
            }
            case None =>
          }

        })
        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
