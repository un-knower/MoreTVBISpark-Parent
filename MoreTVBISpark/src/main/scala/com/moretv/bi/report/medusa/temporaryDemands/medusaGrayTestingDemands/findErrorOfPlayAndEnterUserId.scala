package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 */
object findErrorOfPlayAndEnterUserId extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")

        val startDate = p.startDate

        val enterPath = s"/log/medusa/parquet/$startDate/enter"
        val playPath = s"/log/medusa/parquet/$startDate/play"


        val enterLog = sqlContext.read.parquet(enterPath).select("userId","apkVersion","productModel")
          .registerTempTable("log1")
        val playLog = sqlContext.read.parquet(playPath).select("userId","apkVersion","productModel")
          .registerTempTable("log2")

        val playUserRdd = sqlContext.sql("select distinct userId from log2 where apkVersion='3.0.5' and productModel not" +
          " in ('MagicBox_M13','M321','LetvNewC1S','we20s')").map(e=>e.getString(0))
        val enterUserRdd = sqlContext.sql("select distinct userId from log1 where apkVersion='3.0.5' and productModel" +
          " not in ('MagicBox_M13','M321','LetvNewC1S','we20s')").map(e=>e.getString(0))

        val intersectionRdd = enterUserRdd.intersection(playUserRdd)
        val intersectionArr = intersectionRdd.collect()

        val minIdNum = util.selectOne("select min(id) from tvservice.mtv_account")(0).toString.toLong
        val maxIdNum = util.selectOne("select max(id) from tvservice.mtv_account")(0).toString.toLong

        val userInfoRdd = new JdbcRDD(sc,
          () =>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "select distinct user_id from tvservice.mtv_account where current_version like '%3.0.5' and id >= ? and id<=?",
          minIdNum,
          maxIdNum,
          20,
          r=>(r.getString(1))
        )

        val userInfoArr = userInfoRdd.collect()
        var i=0
        var j=0

        playUserRdd.collect().foreach(u=>{
          if(!intersectionArr.contains(u)){
            i=i+1
            if(!userInfoArr.contains(u)){
              j=j+1
            }
          }
        })

        println("=================")
        println("非交叉用户数量： "+i)
        println("=================")
        println("非交叉用户不在库中的数量： "+j)

      }
      case None => {throw new RuntimeException("At least needs one param:startDate!")}
    }
  }
}
