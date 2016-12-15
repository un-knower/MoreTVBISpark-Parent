package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, JdbcRDD}
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/15.
 * 该对象用于统计每天升级apk的用户信息
 */
object medusaMacToUserIdInfoFromMtvAccount extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val startDate=p.startDate
        val macDir = s"/log/test/parquet/$startDate"
//        val outputPath = s"/log/medusa/mac2UserId/$startDate/userId"
        val outputPath1 = s"/log/medusa/mac2UserId/$startDate/userIdWiFiMac"

        val minIdNum = util.selectOne("select min(id) from mtv_account")(0).toString.toLong
        val maxIdNum = util.selectOne("select max(id) from mtv_account")(0).toString.toLong
        val numOfPartition =20

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
//        val macUserFilterRdd = macUserRdd.map(e=>((e._1,e._2),e._3))
        val wifiMacMacUserFilterRdd = macUserRdd.map(e=>(e._1,e._2,e._3))

        /**
         * 读取mac信息
         */
        sqlContext.read.parquet(macDir).registerTempTable("macTab")
//        val macRdd = sqlContext.sql("select mac,WifiMac,version from macTab")
//          .map(e=>(e.getString(0),e.getString(1),e.getString(2))).map(e=>((e._1,e._2),e._3))
        val wifiMacMacRdd = sqlContext.sql("select mac,WifiMac,version from macTab where version ='3.0.6'")
          .map(e=>(e.getString(0),e.getString(1),e.getString(2))).map(e=>(e._1,e._2,e._3))


//        val mac2UserIdRdd = macRdd.join(macUserFilterRdd)
//        val mac2UserIdMapRdd = mac2UserIdRdd.map(e=>(e._1._1,e._1._2,e._2._2,e._2._1))



        /**
         * 先匹配wifiMac，再匹配Mac
         *
         *
         */
        val wifiMacArr = wifiMacMacRdd.map(e=>e._2).collect()
        val macArr = wifiMacMacRdd.map(e=>e._1).collect()


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

//        val infoToDf = mac2UserIdMapRdd.toDF("mac","wifiMac","userId","version")
//        infoToDf.write.parquet(outputPath)

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
