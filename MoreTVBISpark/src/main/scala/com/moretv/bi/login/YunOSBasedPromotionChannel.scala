package com.moretv.bi.login

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.account.AccountAccess._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
  * Created by zhangyu on 2016/7/18.
  * 统计分渠道的yunos日新增、活跃用户数，采用mac去重方式。
  * tablename: medusa.medusa_yunos_based_promotionchannel
  * (id,day,promotion_channel,adduser_num,active_num)
  * Params : startDate, numOfDays(default = 1);
  *
  */
object YunOSBasedPromotionChannel extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)

  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps("moretv_medusa_mysql")


        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal1.add(Calendar.DAY_OF_MONTH, -1)

        val cal2 = Calendar.getInstance()
        cal2.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {

          val addlogdate = DateFormatUtils.readFormat.format(cal1.getTime)
          val addtimeday = DateFormatUtils.toDateCN(addlogdate)
          val startTime = s"$addtimeday" + " " + "00:00:00"
          val endTime = s"$addtimeday" + " " + "23:59:59"

          val activelogdate = DateFormatUtils.readFormat.format(cal2.getTime)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MTVACCOUNT, addlogdate)
            .select("current_version", "openTime", "mac", "promotion_channel")
            .registerTempTable("addlog_data")

          val addmap = sqlContext.sql(
            s"""
               |  select
               |  case when promotion_channel is null then 'null'
               |  case when promotion_channel = '' then '',count(distinct mac)
               |  from addlog_data
               |  where openTime between '$startTime' and '$endTime'
               |  and (current_version like '%YunOS%' or current_version like 'Alibaba')
               |  group by promotion_channel
             """.stripMargin)
            .collectAsList
            .map(e =>
              ( {
                if (e.getString(0) == null)
                  "null"
                else if (e.getString(0).isEmpty()) "kong"
                else e.getString(0)
              }, e.getLong(1)))
            .toMap

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
            .select("version", "mac", "promotionChannel")
            .registerTempTable("activelog_data")

          val activemap = sqlContext.sql(
            s"""
               |select promotionChannel,count(distinct mac) from activelog_data
               | where version like '%YunOS%' or version like '%Alibaba%' group by promotionChannel
             """.stripMargin).collectAsList
            .map(e =>
              ( {
                if (e.getString(0) == null) "null"
                else if (e.getString(0).isEmpty()) "kong"
                else e.getString(0)
              }, e.getLong(1)))
            .toMap

          if (p.deleteOld) {
            val deleteSql = "delete from medusa_yunos_based_promotionchannel where day = ?"
            util.delete(deleteSql, addtimeday)
          }

          val insertSql = "insert into medusa_yunos_based_promotionchannel(day,promotion_channel,adduser_num,active_num) " +
            "values(?,?,?,?)"

          val keys = addmap.keySet.union(activemap.keySet)
          keys.foreach(key => {
            val adduser_num = addmap.getOrElse(key, 0)
            val active_num = activemap.getOrElse(key, 0)
            util.insert(insertSql, addtimeday, key, adduser_num, active_num)
          })

          cal1.add(Calendar.DAY_OF_MONTH, -1)
          cal2.add(Calendar.DAY_OF_MONTH, -1)
        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}

