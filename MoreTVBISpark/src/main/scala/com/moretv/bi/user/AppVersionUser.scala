package com.moretv.bi.user

import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * 创建人：连凯
  * 创建时间：2016-05-03
  * 程序用途：统计各版本截止某天的总用户数
  * 数据来源：2-15 tvservice用户库
 */
object AppVersionUser extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        val yesterday = DateFormatUtils.enDateAdd(inputDate,-1)
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val userNumMap = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,yesterday).
          filter(s"openTime <= '$day 23:59:59'").
          select("ip","user_id").
          map(row => {
            val version = row.getString(0)
            val mac = row.getString(1)
            if(version == null) ("null",mac) else (version,mac)
          }).distinct.countByKey

        sc.stop()

        if(p.deleteOld) {
          val sqlDelete = s"delete from app_version_user where day = '$day'"
          util.delete(sqlDelete)
        }
        //插入数据库，插入日期，版本号，总用户数
        val sqlInsert = "INSERT INTO app_version_user(day,app_version,user_num) VALUES(?,?,?)"
        userNumMap.foreach(e => {
          util.insert(sqlInsert,day,e._1,e._2)
        })

      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}
