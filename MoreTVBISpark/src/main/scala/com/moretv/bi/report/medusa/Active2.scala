package com.moretv.bi.report.medusa

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/5/10.
  */
object Active2 extends SparkSetting{

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "10g").
      set("spark.cores.max", "100")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    sqlContext.read.load("/report/medusa/userIds2/sample2").registerTempTable("sample")
    val totalUser = sqlContext.sql("select count(userId) from sample").first().getLong(0)
    sqlContext.read.load("/report/medusa/userIds2/medusa2").registerTempTable("medusa")
    val medusaMap = sqlContext.sql("select a.date,count(a.userId) from medusa a join sample b on a.userId = b.userId group by a.date").collect()
    println("=============Medusa============")
    medusaMap.foreach(row => println(row.getString(0),row.getLong(1),totalUser,row.getLong(1).toDouble/totalUser))

    sqlContext.read.load("/report/medusa/userIds2/moretv2").registerTempTable("moretv")
    val moretvMap = sqlContext.sql("select a.date,count(a.userId) from moretv a join sample b on a.userId = b.userId group by a.date").collect()
    println("=============Moretv============")
    moretvMap.foreach(row => println(row.getString(0),row.getLong(1),totalUser,row.getLong(1).toDouble/totalUser))

  }

}
