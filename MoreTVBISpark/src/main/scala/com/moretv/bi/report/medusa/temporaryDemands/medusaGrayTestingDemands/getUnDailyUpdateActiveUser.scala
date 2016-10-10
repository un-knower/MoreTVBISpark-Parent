package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/15.
 * 该对象用于统计每天升级apk的用户信息
 */
object getUnDailyUpdateActiveUser extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = new DBOperationUtils("tvservice")
        val startDate=p.startDate
        val activeUserDir = s"/log/medusa/parquet/$startDate/*"
        val moretvActiveUserDir = s"/mbi/parquet/*/$startDate"
        val dailyUpdateDir = s"/log/medusa/mac2UserId/$startDate/dailyUpdateUserId"
        val outputPath1 = s"/log/medusa/mac2UserId/$startDate/medusaDailyUnUpdateActiveUserId"
        val outputPath2 = s"/log/medusa/mac2UserId/$startDate/moretvDailyUnUpdateActiveUserId"

        (0 until p.numOfDays).foreach(i=>{
          sqlContext.read.parquet(activeUserDir).registerTempTable("log1")
          sqlContext.read.parquet(dailyUpdateDir).registerTempTable("log2")
          sqlContext.read.parquet(moretvActiveUserDir).registerTempTable("log3")
          val activeUserDirRdd = sqlContext.sql("select distinct userId from log1 where apkVersion = '3.0.6'").map(e=>e.getString(0))
          val moretvUserDirRdd = sqlContext.sql("select distinct userId from log3 where apkVersion = '2.6.7'").map(e=>e
            .getString(0))
          val dailyUpdateDirRdd = sqlContext.sql("select distinct userId from log2").map(e=>e.getString(0))

          val dailyUnUpdateActiveUserRdd = activeUserDirRdd.subtract(dailyUpdateDirRdd)
          val moretvDailyUnUpdateActiveUserRdd = moretvUserDirRdd.subtract(dailyUpdateDirRdd)
          val result = dailyUnUpdateActiveUserRdd.toDF("userId")
          val moretvResult = moretvDailyUnUpdateActiveUserRdd.toDF("userId")

          result.write.parquet(outputPath1)
          moretvResult.write.parquet(outputPath2)
        })
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
