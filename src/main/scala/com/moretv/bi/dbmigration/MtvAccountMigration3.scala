package com.moretv.bi.dbmigration

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

import scala.collection.JavaConversions._

/**
  * Created by 连凯 on 2017/7/22.
  * 将原有的mtv_account表中的用户通过旧的新增算法，重新迁移至另一张表中。
  * tablename: medusa.mtv_account_migration
  * (id,user_id,mac,wifi_mac,openTime,promotion_channel,promotion_channel_origin)
  * Params : startDate, numOfDays(default = 1)此程序为正向循环日期递增
  *
  */
object MtvAccountMigration3 extends BaseClass {

  private val wifiMacList = List("000000000000",
    "020000000000",
    "B84D5685FC99",
    "001A34BD9462",
    "00904CC51238",
    "00E04C819200",
    "00E04C870000",
    "112233445566",
    "AABBCCDDEEFF",
    "FFFFFFFFFFFF")

  private val pmList = List("MBXreferenceboard(g18ref)",
    "K200",
    "EC6108V9U_pub_sdlyd",
    "EC6108V9_pub_hnydx",
    "TV628",
    "TV918",
    "TOPBOX_RK3128",
    "BSLA_RK3128",
    "BSLYUN_RK3128",
    "BSL_R10",
    "Nexus11",
    "3128_FG",
    "BS_3128M",
    "BS_3128MF",
    "DYOS",
    "EGREAT_TVBOX3128",
    "gb",
    "INPHIC_RK3128",
    "KBSBOX",
    "MOS-B43",
    "OTT_RK3128",
    "PULIER_3128A",
    "TS_3128A",
    "TXCZ-SDK",
    "VAM_3128",
    "VAM_3128G",
    "VAM_3128J",
    "VAM_3128Y",
    "VAY_3128",
    "VA_3128",
    "YBKJ_K31")

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
          val querySql = "select id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model" +
            s" from mtv_account where openTime between '$startTime' and '$endTime' "
          val rs = mtvAccountDb.selectArrayList(querySql).map(arr => {
            val id = arr(0).toString.toLong
            val user_id = arr(1).toString
            val mac = arr(2) match {
              case null => null
              case x => x.toString
            }
            val wifi_mac = arr(3) match {
              case null => null
              case x => x.toString
            }
            val openTime = arr(4).toString
            val promotion_channel = arr(5) match {
              case null => null
              case x => x.toString
            }
            val product_model = arr(6) match {
              case null => null
              case x => x.toString
            }
            (id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model)
          }).toList

          val dataArr = sc.parallelize(rs,200).mapPartitions(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
            par.map(t => {
              val (id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model) = t
              val (wifi_mac1,wifi_mac2,wifi_mac3) = getWifiMacs(wifi_mac)
              val flag = if(wifi_mac == null || wifi_mac.isEmpty || wifiMacList.contains(wifi_mac2)){
                val sqlCon = " mac = ? and product_model = ? and id < ? limit 1"
                val validationSql = s"select id from mtv_account where $sqlCon"
                val result = db.selectArrayList(validationSql,mac,product_model,id).nonEmpty
                System.err.println(validationSql,mac,product_model,id,result)
                result
              }else {
                if(product_model != null && product_model.nonEmpty && pmList.contains(product_model)){
                  val sqlCon = " wifi_mac in (?,?,?) and product_model = ? and id < ? limit 1"
                  val validationSql = s"select id from mtv_account where $sqlCon"

                  val result = db.selectArrayList(validationSql,wifi_mac1,wifi_mac2,wifi_mac3,product_model,id).nonEmpty
                  System.err.println(validationSql,wifi_mac1,wifi_mac2,wifi_mac3,product_model,id,result)
                  result
                }else{
                  val sqlCon = " wifi_mac in (?,?,?) and mac = ? and product_model = ? and id < ? limit 1"
                  val validationSql = s"select id from mtv_account where $sqlCon"
                  val result = db.selectArrayList(validationSql,wifi_mac1,wifi_mac2,wifi_mac3,mac,product_model,id).nonEmpty
                  System.err.println(validationSql,wifi_mac1,wifi_mac2,wifi_mac3,mac,product_model,id,result)
                  result
                }
              }

              (flag,id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model)
            })
          })

          if (p.deleteOld) {
            val deleteSql1 = s"delete from mtv_account_migration2 where openTime between '$startTime' and '$endTime' "
            val deleteSql2 = s"delete from mtv_account_migration_vice2 where openTime between '$startTime' and '$endTime' "
            util.delete(deleteSql1)
            util.delete(deleteSql2)
          }

          dataArr.foreachPartition(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            val insertSql1 = "insert into mtv_account_migration_vice2(id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model) values(?,?,?, ?,?,?,?) "
            val insertSql2 = "insert into mtv_account_migration2(id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model) values(?,?,?, ?,?,?,?) "
            par.foreach(t => {
              val (flag,id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model) = t
              if(flag){
                db.insert(insertSql1,id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model)
              }else {
                db.insert(insertSql2,id,user_id,mac,wifi_mac,openTime,promotion_channel,product_model)
              }
            })
            db.destory()
          })
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

  def getWifiMacs(wifiMac:String):(String,String,String) = {
    if(wifiMac.contains(":")){
      val wifiMacChars = wifiMac.replace(":","")
      (wifiMac.toUpperCase,wifiMacChars.toUpperCase(),wifiMacChars.toLowerCase())
    }else {
      val sb = new StringBuffer()
      val length = wifiMac.length
      for(i <- 0 until length){
        sb.append(wifiMac.charAt(i))
        if(i % 2 == 1 && i < length - 1) sb.append(":")
      }
      (sb.toString.toUpperCase(),wifiMac.toUpperCase(),wifiMac.toLowerCase())
    }
  }
}
