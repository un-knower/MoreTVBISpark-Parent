package com.moretv.bi.dbmigration

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._

/**
  * Created by 连凯 on 2017/07/22.
  * 通过mtv_account_migration表来计算渠道新增
  */
object PromotionChannelDetailOpen extends BaseClass {


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

          val medusaDb = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val pcSql =
            s"""
              | SELECT ifnull(promotion_channel,'null') as pchannel, COUNT(DISTINCT mac) as new_num
              | FROM mtv_account_migration
              | WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59'
              | GROUP BY promotion_channel
            """.stripMargin

          val pcMap = medusaDb.selectArrayList(pcSql).map(arr => {
            val promotionChannel = arr(0).toString
            val newNum = arr(1).toString.toInt
            if (promotionChannel == "") ("kong", newNum) else (promotionChannel, newNum)
          }).toMap

          val db = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)

          if (p.deleteOld) {
            val sqlDelete = "delete from promotion_detail_open where day = ?"
            db.delete(sqlDelete, day)
          }

          val sqlInsert =
            """
              |insert into promotion_detail_open(year,month,day, promotion_channel,new_num)
              |values(?,?,? ,?,?)
            """.stripMargin

          val year = day.substring(0, 4).toInt
          val month = day.substring(5, 7).toInt

          pcMap.foreach(e => {
            val promotionChannel = e._1
            val newnum = e._2

            db.insert(sqlInsert, year,month, day, promotionChannel,newnum)

          })

          medusaDb.destory()
          db.destory()
          calendar.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
