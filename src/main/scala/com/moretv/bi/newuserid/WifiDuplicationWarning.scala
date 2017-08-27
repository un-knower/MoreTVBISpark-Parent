package com.moretv.bi.newuserid


import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by 连凯 on 2017/8/23.
  * 监控新增用户中的异常现象，重复wifimac和重复IP来源等
  * tablename: medusa.wifi_duplication_warning
  * (id,start_end_date,type,wifi_mac,promotion_channel,product_model,current_ip,duplication_num)
  * Params : startDate, paramMap(pastDays,wifiThreshold,ipThreshold)
  *
  */
object WifiDuplicationWarning extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val wifiThreshold = p.paramMap.getOrElse("wifiThreshold","5").toInt
        val ipThreshold = p.paramMap.getOrElse("ipThreshold","20").toInt
        val endDate = DateFormatUtils.enDateAdd(p.startDate,-1)
        val pastDays = p.paramMap.getOrElse("pastDays","7").toInt
        val startDate = DateFormatUtils.enDateAdd(endDate,-pastDays)
        val startDay = DateFormatUtils.toDateCN(startDate)
        val endDay = DateFormatUtils.toDateCN(endDate)

        if(p.deleteOld){
          val deleteSql = "delete from wifi_duplication_warning where start_end_date = ?"
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          util.delete(deleteSql,s"${startDay}_$endDay")
          util.destory()
        }

        DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,endDate).
          select("wifi_mac","promotion_channel","product_model","ip","openTime").
          filter("wifi_mac is not null and wifi_mac <> ''").
          registerTempTable("snapshot_data")

        sqlContext.sql(s"select distinct wifi_mac from snapshot_data where openTime between '$startDay 00:00:00' and " +
          s"'$endDay 23:59:59'").registerTempTable("new_user_wifi_data")

        sqlContext.sql(s"select a.wifi_mac,a.num from (select wifi_mac,count(0) as num from snapshot_data group by wifi_mac " +
          s"having num >= $wifiThreshold) a join new_user_wifi_data b on a.wifi_mac = b.wifi_mac").
          registerTempTable("exception_wifi_data")

        sqlContext.cacheTable("exception_wifi_data")

        sqlContext.sql("select * from exception_wifi_data").foreachPartition(par => {

          val insertSqlBuffer = new StringBuffer("insert into wifi_duplication_warning(type,start_end_date,wifi_mac,duplication_num) values")
          par.foreach(row => {
            val wifiMac = replaceInvalidChars(row.getString(0))
            val duplicationNum = row.getLong(1)
            insertSqlBuffer.append(s"(1,'${startDay}_$endDay','$wifiMac',$duplicationNum),")

          })
          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          insertSqlBuffer.deleteCharAt(insertSqlBuffer.length()-1)
          db.insert(insertSqlBuffer.toString)
          db.destory()
        })

        sqlContext.sql(s"select a.wifi_mac,a.promotion_channel,a.num from (select wifi_mac,promotion_channel,count(0) as num from snapshot_data group by wifi_mac,promotion_channel) a " +
          s"join exception_wifi_data b on a.wifi_mac = b.wifi_mac").foreachPartition(par => {

          val insertSqlBuffer = new StringBuffer("insert into wifi_duplication_warning(type,start_end_date,wifi_mac,promotion_channel,duplication_num) values")
          par.foreach(row => {
            val wifiMac = replaceInvalidChars(row.getString(0))
            val promotionChannel = replaceInvalidChars(row.getString(1))
            val duplicationNum = row.getLong(2)
            if(promotionChannel != null){
              insertSqlBuffer.append(s"(2,'${startDay}_$endDay','$wifiMac','$promotionChannel',$duplicationNum),")
            }else {
              insertSqlBuffer.append(s"(2,'${startDay}_$endDay','$wifiMac',null,$duplicationNum),")
            }
          })
          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          insertSqlBuffer.deleteCharAt(insertSqlBuffer.length()-1)
          db.insert(insertSqlBuffer.toString)
          db.destory()
        })

        sqlContext.sql(s"select a.wifi_mac,a.product_model,a.num from (select wifi_mac,product_model,count(0) as num from snapshot_data group by wifi_mac,product_model) a " +
          s"join exception_wifi_data b on a.wifi_mac = b.wifi_mac").foreachPartition(par => {

          val insertSqlBuffer = new StringBuffer("insert into wifi_duplication_warning(type,start_end_date,wifi_mac,product_model,duplication_num) values")
          par.foreach(row => {
            val wifiMac = replaceInvalidChars(row.getString(0))
            val productModel = replaceInvalidChars(row.getString(1))
            val duplicationNum = row.getLong(2)
            if(productModel != null){
              insertSqlBuffer.append(s"(3,'${startDay}_$endDay','$wifiMac','$productModel',$duplicationNum),")
            }else insertSqlBuffer.append(s"(3,'${startDay}_$endDay','$wifiMac',null,$duplicationNum),")
          })
          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          insertSqlBuffer.deleteCharAt(insertSqlBuffer.length()-1)
          db.insert(insertSqlBuffer.toString)
          db.destory()
        })

        sqlContext.sql(s"select ip,count(0) as num from snapshot_data where openTime between '$startDay 00:00:00' and " +
          s"'$endDay 23:59:59' group by ip having num >= $ipThreshold").foreachPartition(par => {

          val insertSqlBuffer = new StringBuffer("insert into wifi_duplication_warning(type,start_end_date,current_ip,duplication_num) values")
          par.foreach(row => {
            val currentIp = row.getString(0)
            val duplicationNum = row.getLong(1)
            if(currentIp != null){
              insertSqlBuffer.append(s"(4,'${startDay}_$endDay','$currentIp',$duplicationNum),")
            }else insertSqlBuffer.append(s"(4,'${startDay}_$endDay',null,$duplicationNum),")

          })
          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          insertSqlBuffer.deleteCharAt(insertSqlBuffer.length()-1)
          db.insert(insertSqlBuffer.toString)
          db.destory()
        })

        sqlContext.uncacheTable("exception_wifi_data")

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }

  /**
    * 替换非法字符，避免拼sql在执行时报错
    */
  def replaceInvalidChars(src:String):String = {
    if(src != null)src.replace("\\","\\\\").replace("'","\\'") else null
  }
}
