package com.moretv.bi.dbOperation.db

import java.sql.DriverManager
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.ParamsParseUtil._
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/6/3.
  * This program was to get a snapshot from db which can keep the status of db at the specific moment.
  */
object DBSnapShot extends SparkSetting{

  def main(args: Array[String]) {
    withParse(args){
      p => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH,-1)

        /*(0 until p.numOfDays).foreach(e=>{
          val day = DateFormatUtils.readFormat.format(cal.getTime)
          val dayCN = DateFormatUtils.cnFormat.format(cal.getTime)
          val db2 = DataIO.getMySqlOps("helios_terminal_upgrade_mysql")
          val (min2,max2) = db2.queryMaxMinID("mtv_terminal","id")
          db2.destory()
          val heliosSqlRdd = new JdbcRDD(sc, ()=>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2.18:3306/terminal_upgrade?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
              "whaleybi", "play4bi@whaley")
          },
            "SELECT id,service_id,mac,android_version,open_time,login_time,activate_status,promotion_channel, " +
              "rom_version,status,serial_number,wifi_mac,current_ip " +
              s"FROM `mtv_terminal` WHERE ID >= ? AND ID <= ? and open_time <= '$dayCN 23:59:59'",
            min2,
            max2,
            100,
            r=>(r.getInt(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5),r.getString(6)
              ,r.getInt(7),r.getString(8),r.getString(9),r.getInt(10),r.getString(11),r.getString(12),r.getString(13)))
          val heliosDF = heliosSqlRdd.toDF("id","service_id","mac","android_version","open_time","login_time",
            "activate_status","promotion_channel","rom_version","status","serial_number","wifi_mac","current_ip")
          val outputPath2 = s"/log/dbsnapshot/parquet/$day/helios_mtv_terminal"
          if(p.deleteOld) HdfsUtil.deleteHDFSFile(outputPath2)
          heliosDF.write.parquet(outputPath2)
          cal.add(Calendar.DAY_OF_MONTH,-1)
        })*/


        val db = DataIO.getMySqlOps("moretv_tvservice_mysql")
        (0 until p.numOfDays).foreach(e=>{
          val day = DateFormatUtils.readFormat.format(cal.getTime)
          val dayCN = DateFormatUtils.cnFormat.format(cal.getTime)
          val (min,max) = db.queryMaxMinID("mtv_account","id")
          db.destory()
          val moretvSqlRdd = new JdbcRDD(sc, ()=>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
          },
          "SELECT id,user_id,mac,openTime,lastLoginTime,ip,product_model,product_serial, " +
          "userType,wifi_mac,promotion_channel,current_version,origin_type,sn " +
          s"FROM `mtv_account` WHERE ID >= ? AND ID <= ? and openTime <= '$dayCN 23:59:59'",
              min,
              max,
          300,
              r=>(r.getInt(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5),r.getString(6)
            ,r.getString(7),r.getString(8),r.getInt(9),r.getString(10),r.getString(11),r.getString(12),
                r.getString(13),r.getString(14)))
          val moretvDF = moretvSqlRdd.toDF("id","user_id","mac","openTime","lastLoginTime","ip","product_model","product_serial",
          "userType","wifi_mac","promotion_channel","current_version","origin_type","sn")
          val outputPath = s"/log/dbsnapshot/parquet/$day/moretv_mtv_account"
          if(p.deleteOld) HdfsUtil.deleteHDFSFile(outputPath)
          moretvDF.write.parquet(outputPath)
          cal.add(Calendar.DAY_OF_MONTH,-1)
        })

      }
    }
  }
}
