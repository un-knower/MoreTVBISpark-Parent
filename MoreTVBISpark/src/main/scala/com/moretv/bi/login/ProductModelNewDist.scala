package com.moretv.bi.login


import java.sql.SQLException

import cn.whaley.sdk.dataexchangeio.DataIO

import scala.collection.JavaConversions._
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * 创建人：连凯
  * 创建时间：2016-04-16
  * 程序作用：统计新增用户的终端型号分布
  * 输入数据为：数据库快照，输出到mysql
  *
  */
object ProductModelNewDist extends BaseClass {

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = DateFormatUtils.enDateAdd(p.startDate, -1)
        val day = DateFormatUtils.toDateCN(p.startDate, -1)

        DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,inputDate)
          .registerTempTable("log_data")

        val result = sqlContext.sql("select product_model,count(distinct mac) from log_data " +
          s"where openTime between '$day 00:00:00' and '$day 23:59:59' group by product_model")
          .collectAsList()

        val db = DataIO.getMySqlOps("moretv_eagletv_mysql")
        if (p.deleteOld) {
          val sqlDelete = "delete from Device_Terminal_added where day = ?"
          db.delete(sqlDelete, day)
        }

        val sqlInsert = "insert into Device_Terminal_added(day,product_model,user_num) values(?,?,?)"
        result.foreach(row => {
          try {
            db.insert(sqlInsert, day, row.get(0), row.get(1))
          } catch {
            case e: SQLException =>
              if (e.getMessage.contains("Data too long"))
                println(e.getMessage)
              else throw new RuntimeException(e)
            case e: Exception => throw new RuntimeException(e)
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
