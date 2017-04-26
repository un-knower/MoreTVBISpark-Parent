package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by zhu.bingxin on 2017/4/20.
  * 用户升级情况的统计
  * 按日新版本升级人数
  * 按日新版本累计人数 & 累计人数占比
  */
object UserUpdateStatistic extends BaseClass {
  private val tableName = "whiteMedusa_user_update_statistic"
  private val insertSql = s"insert into ${tableName}(day,userupdate_cnt,proportion) values (?,?,?)"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 to p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, 0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date).
            registerTempTable(LogTypes.MORETV_MTV_ACCOUNT)


          /** 用户升级情况的统计 **/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select count(distinct case when current_version >= '3.14'
               |       and openTime between '$insertDate 00:00:00' and '$insertDate 23:59:59' then mac end) as userupdate_cnt,
               |       count(distinct case when current_version >= '3.14' then mac end)/count(distinct mac) as proportion
               |from ${LogTypes.MORETV_MTV_ACCOUNT}
            """.stripMargin)

          updateUserCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate,e.get(0), e.get(1))
          })
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}