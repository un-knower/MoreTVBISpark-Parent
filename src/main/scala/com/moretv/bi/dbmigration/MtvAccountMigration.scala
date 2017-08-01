package com.moretv.bi.dbmigration

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
  * Created by 连凯 on 2017/7/22.
  * 将原有的mtv_account表中的用户通过旧的新增算法，重新迁移至另一张表中。
  * tablename: medusa.mtv_account_migration
  * (id,user_id,mac,wifi_mac,openTime,promotion_channel,promotion_channel_origin)
  * Params : startDate, numOfDays(default = 1)此程序为正向循环日期递增
  *
  */
object MtvAccountMigration extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val mtvAccountDb = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)

        (0 until p.numOfDays).foreach(i => {
          val addlogdate = DateFormatUtils.readFormat.format(cal.getTime)
          val addtimeday = DateFormatUtils.toDateCN(addlogdate)
          val startTime = s"$addtimeday 00:00:00"
          val endTime = s"$addtimeday 23:59:59"

          val accMtvAccount = sc.accumulator(0,"accMtvAccount")
          val accMtvAccountMigration = sc.accumulator(0,"accMtvAccountMigration")
          val accMtvAccountMigrationVice = sc.accumulator(0,"accMtvAccountMigrationVice")
          val accMtvAccountMigrationClean = sc.accumulator(0,"accMtvAccountMigrationClean")

          val dataRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,addlogdate).
            filter(s"openTime between '$startTime' and '$endTime'").
            select("id","user_id","mac","wifi_mac","openTime","promotion_channel").
            repartition(200).
            mapPartitions(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            par.map(row => {
              accMtvAccount += 1
              val id = row.getInt(0)
              val user_id = row.getString(1)
              val mac = row.getString(2)
              val wifi_mac = row.getString(3)
              val openTime = row.getString(4)
              val promotion_channel = row.getString(5)
              val macCondition = mac != null && mac.nonEmpty
              val wifiMacCondition = wifi_mac != null && wifi_mac.nonEmpty && wifi_mac != "020000000000" && wifi_mac != "02:00:00:00:00:00"
              val sqlCon = if(macCondition && wifiMacCondition) s" mac = '$mac' or wifi_mac = '$wifi_mac'"
                else if(macCondition && !wifiMacCondition) s" mac = '$mac'"
                else if(!macCondition && wifiMacCondition) s" wifi_mac = '$wifi_mac'"
                else ""

              val flag = if(sqlCon != "") {
                val validationSql = s"select id from mtv_account_migration where $sqlCon"
                db.selectArrayList(validationSql).exists(x => x(0).toString.toLong < id)
              }else false

              (flag,id,user_id,mac,wifi_mac,openTime,promotion_channel)
            })
          }).persist(StorageLevel.MEMORY_AND_DISK)

          if (p.deleteOld) {
            val deleteSql1 = s"delete from mtv_account_migration where openTime between '$startTime' and '$endTime' "
            val deleteSql2 = s"delete from mtv_account_migration_vice where openTime between '$startTime' and '$endTime' "
            util.delete(deleteSql1)
            util.delete(deleteSql2)
          }

          dataRdd.foreachPartition(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            val insertSql1 = "insert into mtv_account_migration_vice(id,user_id,mac,wifi_mac,openTime,promotion_channel) values(?,?,?, ?,?,?) "
            val insertSql2 = "insert into mtv_account_migration(id,user_id,mac,wifi_mac,openTime,promotion_channel) values(?,?,?, ?,?,?) "
            par.foreach(t => {
              val (flag,id,user_id,mac,wifi_mac,openTime,promotion_channel) = t
              if(flag){
                accMtvAccountMigration += 1
                db.insert(insertSql1,id,user_id,mac,wifi_mac,openTime,promotion_channel)
              }else {
                accMtvAccountMigrationVice += 1
                db.insert(insertSql2,id,user_id,mac,wifi_mac,openTime,promotion_channel)
              }
            })
            db.destory()
          })

          dataRdd.filter(!_._1).mapPartitions(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

            par.map(t => {
              val (_,id,user_id,mac,wifi_mac,openTime,promotion_channel) = t
              val macCondition = mac != null && mac.nonEmpty
              val wifiMacCondition = wifi_mac != null && wifi_mac.nonEmpty && wifi_mac != "020000000000" && wifi_mac != "02:00:00:00:00:00"
              val sqlCon = if(macCondition && wifiMacCondition) s" mac = '$mac' or wifi_mac = '$wifi_mac'"
              else if(macCondition && !wifiMacCondition) s" mac = '$mac'"
              else if(!macCondition && wifiMacCondition) s" wifi_mac = '$wifi_mac'"
              else ""

              val flag = if(sqlCon != "") {
                val validationSql = s"select openTime from mtv_account_migration where $sqlCon"
                db.selectArrayList(validationSql).exists(x => {
                  val time = x(0).toString
                  time >= startTime && time < openTime
                })
              }else false

              if(flag){
                (id,user_id,mac,wifi_mac,openTime,promotion_channel)
              }else null
            })
          }).filter(_!=null).foreachPartition(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            val cleanSql = "delete from mtv_account_migration where id = ? "
            val insertSql = "insert into mtv_account_migration_vice(id,user_id,mac,wifi_mac,openTime,promotion_channel) values(?,?,?, ?,?,?) "
            par.foreach(t => {
              accMtvAccountMigrationClean += 1
              val (id,user_id,mac,wifi_mac,openTime,promotion_channel) = t
              db.insert(insertSql,id,user_id,mac,wifi_mac,openTime,promotion_channel)
              db.delete(cleanSql,id)
            })
            db.destory()
          })


          dataRdd.unpersist()

          System.err.println("#########################################################")
          System.err.println(s"openTime between $startTime and $endTime")
          System.err.println(s"accMtvAccount:\t$accMtvAccount")
          System.err.println(s"accMtvAccountMigration:\t$accMtvAccountMigration")
          System.err.println(s"accMtvAccountMigrationVice:\t$accMtvAccountMigrationVice")
          System.err.println(s"accMtvAccountMigrationClean:\t$accMtvAccountMigrationClean")
          System.err.println("#########################################################")

          cal.add(Calendar.DAY_OF_MONTH, 1)
        })

        mtvAccountDb.destory()
        util.destory()

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}
