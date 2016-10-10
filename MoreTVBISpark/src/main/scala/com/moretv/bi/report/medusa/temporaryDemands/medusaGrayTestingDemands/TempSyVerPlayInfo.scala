package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object TempSyVerPlayInfo extends SparkSetting{
  def main(args: Array[String]) {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")

        val medusaDir = "/log/moretvloginlog/parquet/20160{7[31,30,29,28,27,26],801}"
        val enterLogType = "loginlog"

        val dir = s"$medusaDir/$enterLogType/"
    sqlContext.read.parquet(dir).registerTempTable("log")
    val rdd = sqlContext.sql("select productModel,count(distinct userId) from log " +
      "where SysVer in ('Android_SDK_14','Android_SDK_13','Android_SDK_12','Android_SDK_11','Android_SDK_10'," +
      "'Android_SDK_9','Android_SDK_8') group by productModel").map(e=>(e.getString(0),e.getLong(1)))

    val insertSql = "insert into temp_for_zhouchong(productModel,active_user) values(?,?)"
    rdd.collect().foreach(e=>{
      util.insert(insertSql,e._1,new JLong(e._2))
    })
  }
}
