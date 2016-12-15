package com.moretv.bi.report.medusa

import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Will on 2016/5/10.
  */
object Sample2 extends SparkSetting{

  val pmList = List("WE20S","M321","LETVNEWC1S","MAGICBOX_M13")
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "10g").
      set("spark.cores.max", "100")

    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val sampleActiveRdd = sqlContext.read.load("/log/moretvloginlog/parquet/20160{32[456789],33?,4[0123456]}/loginlog").
      filter("version is not null and userId is not null and productModel is not null").
      select("productModel","version","userId").
      map(row => {
        val pm = row.getString(0).toUpperCase
        val version = row.getString(1)
        if(!pmFilter(pm) && version == "MoreTV_TVApp2.0_Android_2.6.7") row.getString(2) else null
      }).filter(_ != null).distinct()
    val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
    val ids = util.selectOne(s"SELECT MIN(id),MAX(id) FROM tvservice.mtv_account WHERE openTime BETWEEN '2016-03-03 00:00:00' AND '2016-04-05 23:59:59'")
    val sampleNewRdd = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      s"SELECT user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? " +
        s"and openTime between '2016-03-03 00:00:00' and '2016-04-05 23:59:59'",
      ids(0).toString.toLong,
      ids(1).toString.toLong,
      10,
      r=>r.getString(1)).filter(_ != null).distinct()
    val totalRdd = sampleActiveRdd.subtract(sampleNewRdd)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val fraction = 81104.0/totalRdd.count()
    val sampleRdd = totalRdd.sample(false,fraction).distinct()
    sampleRdd.toDF("userId").write.parquet("/report/medusa/userIds2/sample2")
    totalRdd.unpersist()

    val medusaSampleRdd = sqlContext.read.load("/log/moretvloginlog/parquet/20160{40[89],41?,42?,43?,5??}/loginlog").
      filter("version is not null and userId is not null and productModel is not null").
      select("productModel","version","date","userId").
      map(row => {
        val pm = row.getString(0).toUpperCase
        val version = row.getString(1)
        if(!pmFilter(pm) && version == "MoreTV_TVApp2.0_Android_2.6.7") (row.getString(2),row.getString(3)) else null
      }).filter(_ != null).distinct()
    medusaSampleRdd.toDF("date","userId").write.parquet("/report/medusa/userIds2/medusa2")
    val moretvSampleRdd = sqlContext.read.load("/log/moretvloginlog/parquet/201603{0[456789],1?,2[012]}/loginlog").
      filter("version is not null and userId is not null and productModel is not null").
      select("productModel","version","date","userId").
      map(row => {
        val pm = row.getString(0).toUpperCase
        val version = row.getString(1)
        if(!pmFilter(pm) && version == "MoreTV_TVApp2.0_Android_2.6.7") (row.getString(2),row.getString(3)) else null
      }).filter(_ != null).distinct()
    moretvSampleRdd.toDF("date","userId").write.parquet("/report/medusa/userIds2/moretv2")

  }

  def pmFilter(pm:String) = pmList.contains(pm)
}
