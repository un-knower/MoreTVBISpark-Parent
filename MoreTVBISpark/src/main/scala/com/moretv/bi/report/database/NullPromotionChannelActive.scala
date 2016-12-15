package com.moretv.bi.report.database

import java.sql.DriverManager

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Will on 2016/3/24.
  */
object NullPromotionChannelActive extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(NullPromotionChannelActive,args)
  }
  override def execute(args: Array[String]) {
    val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
    val sqlIds = "SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account` "
    val ids = db.selectOne(sqlIds)
    val min = ids(0).toString.toLong
    val max = ids(1).toString.toLong
    val sqlRdd = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
      },
      "SELECT user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and openTime < '2016-04-01 00:00:00' and promotion_channel IS NULL ",
      min,
      max,
      1000,
      r=>r.getString(1)).filter(_ != null).distinct().persist(StorageLevel.MEMORY_AND_DISK)

    val detailRdd = sqlContext.read.load("/mbi/parquet/detail/201[56]*").
      select("userId").map(_.getString(0)).distinct().persist(StorageLevel.MEMORY_AND_DISK)
    val playRdd = sqlContext.read.load("/mbi/parquet/playview/201[56]*").
      select("userId").map(_.getString(0)).distinct().persist(StorageLevel.MEMORY_AND_DISK)

    println("sqlRdd.size:"+sqlRdd.count())
    println("detailRdd.size:"+detailRdd.count())
    println("playRdd.size:"+playRdd.count())
    println("playRdd union detailRdd size:"+playRdd.union(detailRdd).count())

    val result = (detailRdd union playRdd).intersection(sqlRdd).count()
    println("result size:"+result)

  }


}
