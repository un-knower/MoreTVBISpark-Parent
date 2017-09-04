package com.moretv.bi.dbmigration

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
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
object MtvAccountMigration4 extends BaseClass {

  private val wifiMacList = List("000000000000",
    "020000000000",
    "b84d5685fc99",
    "001A34BD9462",
    "00904cc51238",
    "00e04c819200",
    "00e04c870000",
    "112233445566",
    "AABBCCDDEEFF",
    "FFFFFFFFFFFF")

  private val pmList = List("MBXreferenceboard(g18ref)",
    "K200")
//    "EC6108V9U_pub_sdlyd",
//    "EC6108V9_pub_hnydx",
//    "TV628",
//    "TV918",
//    "TOPBOX_RK3128",
//    "BSLA_RK3128",
//    "BSLYUN_RK3128",
//    "BSL_R10",
//    "Nexus11",
//    "3128_FG",
//    "BS_3128M",
//    "BS_3128MF",
//    "DYOS",
//    "EGREAT_TVBOX3128",
//    "gb",
//    "INPHIC_RK3128",
//    "KBSBOX",
//    "MOS-B43",
//    "OTT_RK3128",
//    "PULIER_3128A",
//    "TS_3128A",
//    "TXCZ-SDK",
//    "VAM_3128",
//    "VAM_3128G",
//    "VAM_3128J",
//    "VAM_3128Y",
//    "VAY_3128",
//    "VA_3128",
//    "YBKJ_K31")

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)

        (0 until p.numOfDays).foreach(i => {
          val addlogdate = DateFormatUtils.readFormat.format(cal.getTime)
          val addtimeday = DateFormatUtils.toDateCN(addlogdate)
          val startTime = s"$addtimeday 00:00:00"
          val endTime = s"$addtimeday 23:59:59"

          if (p.deleteOld) {
            val deleteSql = s"delete from mtv_account_migration4 where day = ? "
            util.delete(deleteSql,addtimeday)
          }

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG,addlogdate).
            filter(s"(userId is null or userId = '') and productModel in ('EC6108V9U_pub_sdlyd','EC6108V9_pub_hnydx','TV628','TV918','TOPBOX_RK3128','BSLA_RK3128','BSLYUN_RK3128','BSL_R10','Nexus11','3128_FG','BS_3128M','BS_3128MF','DYOS','EGREAT_TVBOX3128','gb','INPHIC_RK3128','KBSBOX','MOS-B43','OTT_RK3128','PULIER_3128A','TS_3128A','TXCZ-SDK','VAM_3128','VAM_3128G','VAM_3128J','VAM_3128Y','VAY_3128','VA_3128','YBKJ_K31')").
            select("userId","mac","wifiMac","WifiMac","productModel","promotionChannel").
            distinct().
            repartition(200).
            mapPartitions(par => {
              val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
              par.map(row => {
                val user_id = row.getString(0)
                val mac = row.getString(1)
                val wifi_mac = row.getString(2) match {
                  case null => row.getString(3)
                  case x => x
                }
                val product_model = row.getString(4)
                val promotionChannel = row.getString(5)

//                val isExists = if(user_id != null){
//                  val querySql = s"select user_id from mtv_account where user_id = '$user_id'"
//                  db.selectOne(querySql).nonEmpty
//                }else false

//                if(!isExists){
                  val flag = if(wifi_mac == null || wifi_mac.isEmpty || wifiMacList.exists(x => x.equalsIgnoreCase(wifi_mac))){
                    val sqlCon = " mac = ? and product_model = ?"
                    val validationSql = s"select openTime from mtv_account where $sqlCon"
                    db.selectArrayList(validationSql,mac,product_model).exists(x => x.toString <= startTime)
                  }else {
                    val (wifi_mac1,wifi_mac2,wifi_mac3) = getWifiMacs(wifi_mac)
                    val sqlCon = " wifi_mac in (?,?,?) and mac = ? and product_model = ?"
                    val validationSql = s"select openTime from mtv_account where $sqlCon"
                    db.selectArrayList(validationSql,wifi_mac1,wifi_mac2,wifi_mac3,mac,product_model).exists(x => x.toString <= startTime)
                  }
                  if (!flag) (user_id,mac,wifi_mac,product_model,promotionChannel)
                  else null
//                }else null

              })
            }).filter(_!=null).foreachPartition(par => {
            val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            val insertSql = "insert into mtv_account_migration4(day,user_id,mac,wifi_mac,product_model,promotion_channel) values(?,?,?, ?,?,?) "
            par.foreach(t => {
              val (user_id,mac,wifi_mac,product_model,promotionChannel) = t
              db.insert(insertSql,addtimeday,user_id,mac,wifi_mac,product_model,promotionChannel)
            })
            db.destory()
          })

          cal.add(Calendar.DAY_OF_MONTH, 1)
        })

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
