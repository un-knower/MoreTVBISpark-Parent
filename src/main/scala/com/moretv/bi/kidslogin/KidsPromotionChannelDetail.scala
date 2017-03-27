package com.moretv.bi.kidslogin

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/2/16.
  */
object KidsPromotionChannelDetail extends BaseClass {

  val regex = "^\\w+$".r

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)

        val logRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MTVKIDSLOGIN, LogTypes.LOGINLOG)
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
          }).cache()
        val loginNums = logRdd.countByKey()

        val userNums = logRdd.distinct().countByKey()

        val dbTvService = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val pcSql = "SELECT ifnull(promotion_channel,'null') as pchannel, COUNT(DISTINCT mac) as new_num " +
          s"FROM mtv_kid_account WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59' GROUP BY promotion_channel"

        val pcMap = dbTvService.selectArrayList(pcSql).map(arr => (arr(0).toString, arr(1).toString.toInt)).toMap

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        if (p.deleteOld) {
          val sqlDelete = "delete from mtv_kid_promotion_detail where day = ?"
          db.delete(sqlDelete, day)
        }

        val sqlInsert = "insert into mtv_kid_promotion_detail(year,month,day, promotion_channel,new_num,user_num, login_num) " +
          " values(?,?,? ,?,?,? ,?)"
        val year = day.substring(0, 4).toInt
        val month = day.substring(5, 7).toInt
        userNums.foreach(x => {
          val promotionChannel = x._1
          val usernum = x._2
          val loginnum = loginNums(promotionChannel)
          val newnum = pcMap.getOrElse(promotionChannel, 0)
          db.insert(sqlInsert, new Integer(year), new Integer(month), day, promotionChannel, new Integer(newnum), new Integer(usernum.toInt), new Integer(loginnum.toInt))
        })

        dbTvService.destory()
        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
