package com.moretv.bi.login

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.storage.StorageLevel

/**
  * Created by zhangyu on 2016/7/21.
  * 统计每日新增用户的版本、渠道分布，采用mac去重方式。
  * tablename: medusa.medusa_adduser_based_apkversion_and_promotionchannel
  * (id,day,apk_version,promotion_channel,adduser_num)
  * Params : startDate, numOfDays(default = 1);
  *
  */
object AdduserBasedApkVersionAndPromotionChannel extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal1.add(Calendar.DAY_OF_MONTH, -1)

        (0 until p.numOfDays).foreach(i => {
          val addlogdate = DateFormatUtils.readFormat.format(cal1.getTime)
          val addtimeday = DateFormatUtils.toDateCN(addlogdate)
          val startTime = s"$addtimeday" + " " + "00:00:00"
          val endTime = s"$addtimeday" + " " + "23:59:59"

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,addlogdate)
            .select("current_version", "openTime", "mac", "promotion_channel")
            .registerTempTable("addlog_data")

          val addRdd = sqlContext.sql(
            s"""
               | select current_version,promotion_channel,count(distinct mac)
               | from addlog_data
               | where openTime between '$startTime' and '$endTime'
               | group by current_version,promotion_channel
               |
             """.stripMargin)
            .map(e => ( {
              if (e.getString(0) == null) "null" else if (e.getString(0).isEmpty) "kong" else e.getString(0)
            }, {
              if (e.getString(1) == null) "null" else if (e.getString(1).isEmpty) "kong" else e.getString(1)
            },
              e.getLong(2)))
            .persist(StorageLevel.MEMORY_AND_DISK)

          if (p.deleteOld) {
            val deleteSql = "delete from medusa_adduser_based_apkversion_and_promotionchannel where day = ?"
            util.delete(deleteSql, addtimeday)
          }

          val insertSql = "insert into medusa_adduser_based_apkversion_and_promotionchannel(day,apk_version,promotion_channel,adduser_num) " +
            "values(?,?,?,?)"
          addRdd.collect.foreach(e => {
            util.insert(insertSql, addtimeday, e._1, e._2, e._3)
          })
          cal1.add(Calendar.DAY_OF_MONTH, -1)
        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}
