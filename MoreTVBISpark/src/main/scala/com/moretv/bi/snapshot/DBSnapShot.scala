package com.moretv.bi.snapshot

import java.sql.DriverManager
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.{Database, Constants, Tables}
import com.moretv.bi.util.ParamsParseUtil._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil}
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by Will on 2016/6/3.
  * This program was to get a snapshot from db which can keep the status of db at the specific moment.
  */
object DBSnapShot extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    withParse(args) {
      p => {
        val s = sqlContext
        import s.implicits._
        val cal = Calendar.getInstance()
        cal.add(Calendar.DAY_OF_MONTH, -1)
        val day = DateFormatUtils.readFormat.format(cal.getTime)
        val dayCN = DateFormatUtils.cnFormat.format(cal.getTime)

        val db = DataIO.getMySqlOps("moretv_tvservice_mysql")
        val url = db.prop.getProperty("url")
        val driver = db.prop.getProperty("driver")
        val user = db.prop.getProperty("user")
        val password = db.prop.getProperty("password")

        val (min, max) = db.queryMaxMinID("mtv_account", "id")
        db.destory()

        val sqlInfo = "SELECT id,user_id,mac,openTime,lastLoginTime,ip,product_model,product_serial, " +
          "userType,wifi_mac,promotion_channel,current_version,origin_type,sn " +
          s"FROM `mtv_account` WHERE ID >= ? AND ID <= ? and openTime <= '$dayCN 23:59:59'"


        val moretvSqlRdd = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT,
          r => (r.getInt(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getString(6)
            , r.getString(7), r.getString(8), r.getInt(9), r.getString(10), r.getString(11), r.getString(12),
            r.getString(13), r.getString(14)), driver, url, user, password, (min, max), 300)

        val moretvDF = moretvSqlRdd.toDF("id", "user_id", "mac", "openTime", "lastLoginTime", "ip", "product_model", "product_serial",
          "userType", "wifi_mac", "promotion_channel", "current_version", "origin_type", "sn")

        val outputPath = s"/log/dbsnapshot/parquet/$day/moretv_mtv_account"

        if (p.deleteOld) HdfsUtil.deleteHDFSFile(outputPath)

        moretvDF.write.parquet(outputPath)
      }
    }
  }
}
