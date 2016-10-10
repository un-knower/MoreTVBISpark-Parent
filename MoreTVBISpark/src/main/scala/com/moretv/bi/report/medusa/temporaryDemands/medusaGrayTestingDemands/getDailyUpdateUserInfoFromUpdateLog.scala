package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/5/15.
 * 该对象用于统计每天升级apk的用户信息
 */
object getDailyUpdateUserInfoFromUpdateLog extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = new DBOperationUtils("tvservice")
        val startDate=p.startDate
        val macDir = s"/log/moretvupgrade/parquet/$startDate"
        val outputPath1 = s"/log/medusa/mac2UserId/$startDate/dailyUpdateUserId"

        val minIdNum = util.selectOne("select min(id) from mtv_account")(0).toString.toLong
        val maxIdNum = util.selectOne("select max(id) from mtv_account")(0).toString.toLong
        val numOfPartition =200

        val macUserRdd = new JdbcRDD(sc,
          ()=>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "select mac,wifi_mac,user_id from mtv_account where id >= ? and id <= ? and current_version='MoreTV_TVApp3" +
            ".0_Medusa_3.0.6'",
          minIdNum,
          maxIdNum,
          numOfPartition,
          r=>(r.getString(1),r.getString(2),r.getString(3))
        )
        val wifiMacMacUserFilterRdd = macUserRdd.map(e=>(e._1,e._2,e._3))

        /**
         * 读取mac信息
         */
        sqlContext.read.parquet(macDir).registerTempTable("macTab")
        val wifiMacMacRdd = sqlContext.sql("select mac,WifiMac,version from macTab")
          .map(e=>(e.getString(0),e.getString(1),e.getString(2))).map(e=>(e._1,e._2,e._3))

        /**
         * 将当日升级的用户数据过滤出来
         */

        val todayUpdateInfoRdd = wifiMacMacRdd.filter(e=>{!(e._1==null && e._2==null)}).filter(_._3!=null).map(e=>((e._1,e
          ._2),e._3)).groupBy(_._1).map(e=>{
          val list = e._2.toList.map(_._2).distinct.sortBy(e=>e)
          if(list.size>1 && list.contains("3.0.6")) (e._1._1,e._1._2)
          else null
        }).filter(_!=null).persist(StorageLevel.MEMORY_AND_DISK)


        /**
         * 先匹配wifiMac，再匹配Mac
         *
         *
         */
        val wifiMacArr = todayUpdateInfoRdd.map(e=>e._2).collect()
        val macArr = todayUpdateInfoRdd.map(e=>e._1).collect()


        val resultUserRdd = wifiMacMacUserFilterRdd.map(e=>{
          if(existWiFiMac(wifiMacArr,e._2,"wifiMac")){
            e._3
          }else if(existWiFiMac(macArr,e._1,"mac")){
            e._3
          }else{
            null
          }
        })

        val resultUserDf = resultUserRdd.toDF("userId")
        resultUserDf.write.parquet(outputPath1)

        todayUpdateInfoRdd.unpersist()
      }
      case None=>{throw new RuntimeException("At least needs one param: StartDate!")}
    }
  }


  def existWiFiMac(arr:Array[String],wifiMac:String,flag:String)={
    flag match {
      case "wifiMac"=>{
        if(arr.contains(wifiMac)){
          true
        }else{
          false
        }
      }
      case "mac"=>{
        if(arr.contains(wifiMac)){
          true
        }else{
          false
        }
      }
    }

  }
}
