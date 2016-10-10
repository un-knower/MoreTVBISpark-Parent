package com.moretv.bi.report.medusa

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/5/10.
  */
object Active extends SparkSetting{

  val pmList = List("WE20S","M321","LETVNEWC1S","MAGICBOX_M13")

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "10g").
      set("spark.cores.max", "100")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sqlContext.read.load("/report/medusa/userIds/sample3").registerTempTable("sample")
    val totalUser = sqlContext.sql("select count(userId) from sample").first().getLong(0)
    sqlContext.read.load("/report/medusa/userIds/medusa3").registerTempTable("medusa")
    val medusaMap = sqlContext.sql("select a.date,count(a.userId) from medusa a join sample b on a.userId = b.userId group by a.date").collect()
    println("=============Medusa============")
    medusaMap.foreach(row => println(row.getString(0),row.getLong(1),totalUser,row.getLong(1).toDouble/totalUser))

    sqlContext.read.load("/report/medusa/userIds/moretv3").registerTempTable("moretv")
    val moretvMap = sqlContext.sql("select a.date,count(a.userId) from moretv a join sample b on a.userId = b.userId group by a.date").collect()
    println("=============Moretv============")
    moretvMap.foreach(row => println(row.getString(0),row.getLong(1),totalUser,row.getLong(1).toDouble/totalUser))

  }

  def pmFilter(pm:String) = pmList.contains(pm)
}
