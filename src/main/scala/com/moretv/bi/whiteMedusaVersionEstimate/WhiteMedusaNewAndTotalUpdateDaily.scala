package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, ParamsParseUtil}

/**
  * Created by Chubby on 2017/5/8.
  * 该类用于统计升级过3.1.4版本的用户信息
  */
object WhiteMedusaNewAndTotalUpdateDaily extends BaseClass{
  private val insertSql = "insert into white_medusa_new_total_user_by_login(day,new_num,newUpdateUser_UserId,total_num,totalUser_UserId,all_total_num) values(?,?,?,?,?,?)"
  private val deleteSql = "delete from white_medusa_new_total_user_by_login where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i=>{
          val loadDate = DateFormatUtils.readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val insertDate = DateFormatUtils.cnFormat.format(calendar.getTime)
          val dbsnapshotLoadDate = DateFormatUtils.readFormat.format(calendar.getTime)

          /**
            * 加载登录信息
            */
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.WHITE_MEDUSA_UPDATE_USER,loadDate).
            registerTempTable("white_medusa_update_log")

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.WHITE_MEDUSA_UPDATE_USER_BY_UID,loadDate).
            registerTempTable("white_medusa_update_by_uid_log")

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,dbsnapshotLoadDate).
            registerTempTable("account_log")

          val totalUser = sqlContext.sql(
            s"""
               |select distinct mac
               |from white_medusa_update_log
               |where date <= '${insertDate}'
            """.stripMargin).count()

          val totalUser_UserId = sqlContext.sql(
            s"""
               |select distinct userId
               |from white_medusa_update_by_uid_log
               |where date <= '${insertDate}'
            """.stripMargin).count()

          val newUpdateUser = sqlContext.sql(
            s"""
               |select distinct mac
               |from white_medusa_update_log
               |where date = '${insertDate}'
            """.stripMargin).count()

          val newUpdateUser_UserId = sqlContext.sql(
            s"""
               |select distinct userId
               |from white_medusa_update_by_uid_log
               |where date = '${insertDate}'
            """.stripMargin).count()

          val totalAllUser = sqlContext.sql(
            """
              |select distinct mac
              |from account_log
            """.stripMargin).count()
          if(p.deleteOld) util.delete(deleteSql,insertDate)

          util.insert(insertSql,insertDate,newUpdateUser,newUpdateUser_UserId,totalUser,totalUser_UserId,totalAllUser)

        })

      }
      case None => {}
    }
  }
}
