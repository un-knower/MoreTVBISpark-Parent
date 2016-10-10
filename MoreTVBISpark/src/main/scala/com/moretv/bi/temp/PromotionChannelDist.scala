package com.moretv.bi.temp

import java.sql.DriverManager

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
 * Created by 胡浙华 on 2016/6/1.
 */

/**
 * 统计各渠道截止到某天的累计激活用户数
 */
object PromotionChannelDist extends SparkSetting with QueryMaxAndMinIDUtil{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val id = queryID("id","mtv_account","jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");
        config.setAppName("mtv_account")
        val sc = new SparkContext(config)
        //val sqlContext = new SQLContext(sc)

        val queryRDD = new JdbcRDD(sc, ()=>{
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
        },
          "SELECT promotion_channel,mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? AND openTime <= '2016-05-31 23:59:59' " +
            "AND promotion_channel IN ('inphic','diyomate','10moons','kaiboer') AND current_version LIKE '%YunOS%'",
          id(1), id(0), 10,
          r=>(r.getString(1), r.getString(2)))

        println("rdd.count = "+queryRDD.count())
        queryRDD.distinct().countByKey().foreach(println)
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
