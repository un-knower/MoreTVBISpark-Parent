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
object medusaMoretvUserFromMtvAccount extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = new DBOperationUtils("tvservice")
        val startDate=p.startDate

        val outputPath = s"/log/medusa/mac2UserId/$startDate/userIdFromDB"

        val minIdNum = util.selectOne("select min(id) from mtv_account")(0).toString.toLong
        val maxIdNum = util.selectOne("select max(id) from mtv_account")(0).toString.toLong
        val numOfPartition =20

        val macUserRdd = new JdbcRDD(sc,
          ()=>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "select mac,wifi_mac,user_id,current_version from mtv_account where id >= ? and id <= ? and current_version in " +
            "('MoreTV_TVApp3" +
            ".0_Medusa_3.0.6','MoreTV_TVApp2.0_Android_2.6.7')",
          minIdNum,
          maxIdNum,
          numOfPartition,
          r=>(r.getString(1),r.getString(2),r.getString(3),r.getString(4))
        )
        val macUserFilterRdd = macUserRdd.map(e=>(e._1,e._2,e._3,e._4))

        val infoToDf = macUserFilterRdd.toDF("mac","wifiMac","userId","version")
        infoToDf.write.parquet(outputPath)

      }
      case None=>{throw new RuntimeException("At least needs one param: StartDate!")}
    }
  }
}
