package com.moretv.bi.login

import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/2/16.
  */
object PromotionChannelDetail extends BaseClass {

  val regex = "^\\w+$".r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val calendar = Calendar.getInstance()
        val startDay = p.startDate
        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))

        (0 until p.numOfDays).foreach(x => {

          val inputDate = DateFormatUtils.readFormat.format(calendar.getTime)
          val day = DateFormatUtils.toDateCN(inputDate, -1)

          val logRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
            .select("promotionChannel", "mac")
            .map(row => {
              val promotionChannel = row.getString(0)
              if (promotionChannel != null) {
                if (promotionChannel == "") {
                  ("kong", row.getString(1))
                } else {
                  regex findFirstIn promotionChannel match {
                    case Some(s) => (s, row.getString(1))
                    case None => ("corrupt", row.getString(1))
                  }
                }
              } else ("null", row.getString(1))
            })
            .cache()

          val loginNums = logRdd.countByKey()
          val userNums = logRdd.distinct().countByKey()

          val dbTvService = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
          val pcSql =
            s"""
              | SELECT ifnull(promotion_channel,'null') as pchannel, COUNT(DISTINCT mac) as new_num
              | FROM mtv_account
              | WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59'
              | GROUP BY promotion_channel
            """.stripMargin

          val pcMap = dbTvService.selectArrayList(pcSql).map(arr => {
            val promotionChannel = arr(0).toString
            val newNum = arr(1).toString.toInt
            if (promotionChannel == "") ("kong", newNum) else (promotionChannel, newNum)
          }).toMap

          val db = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)

          if (p.deleteOld) {
            val sqlDelete = "delete from promotion_detail where day = ?"
            db.delete(sqlDelete, day)
          }

          val sqlInsert =
            """
              |insert into promotion_detail(year,month,day, promotion_channel,new_num,user_num, login_num)
              |values(?,?,? ,?,?,? ,?)
            """.stripMargin

          val year = day.substring(0, 4).toInt
          val month = day.substring(5, 7).toInt

          pcMap.keySet.union(userNums.keySet).foreach(key => {
            val promotionChannel = key
            val usernum = userNums.getOrElse(promotionChannel, 0L)
            val loginnum = loginNums.getOrElse(promotionChannel, 0L)

            val newnum = pcMap.getOrElse(promotionChannel, 0)

            db.insert(sqlInsert,
              new Integer(year), new Integer(month), day, promotionChannel, new Integer(newnum),
              new Integer(usernum.toInt), new Integer(loginnum.toInt))

          })

          dbTvService.destory()
          db.destory()
          logRdd.unpersist()
          calendar.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
