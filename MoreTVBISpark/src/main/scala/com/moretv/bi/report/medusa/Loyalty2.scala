package com.moretv.bi.report.medusa

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/5/10.
  */
object Loyalty2 extends SparkSetting{


  def main(args: Array[String]) {
    config.set("spark.executor.memory", "10g").
      set("spark.cores.max", "100")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    sqlContext.read.load("/report/medusa/userIds2/sample2").registerTempTable("sample")
    sqlContext.read.load("/report/medusa/userIds2/medusa2").registerTempTable("medusa")
    sqlContext.read.load("/report/medusa/userIds2/moretv2").registerTempTable("moretv")

    val medusaMap1 = sqlContext.sql("select a.userId from medusa a join sample b on a.userId = b.userId " +
      "where a.date between '2016-04-07' and '2016-04-13'").map(_.getString(0)).groupBy(x => x).
      map(e => (e._2.size,e._1)).countByKey()
    val medusaMap2 = sqlContext.sql("select a.userId from medusa a join sample b on a.userId = b.userId " +
      "where a.date between '2016-04-14' and '2016-04-20'").map(_.getString(0)).groupBy(x => x).
      map(e => (e._2.size,e._1)).countByKey()
    val medusaMap3 = sqlContext.sql("select a.userId from medusa a join sample b on a.userId = b.userId " +
      "where a.date between '2016-04-21' and '2016-04-25'").map(_.getString(0)).groupBy(x => x).
      map(e => (e._2.size,e._1)).countByKey()
    val moretvMap1 = sqlContext.sql("select a.userId from moretv a join sample b on a.userId = b.userId " +
      "where a.date between '2016-03-03' and '2016-03-09'").map(_.getString(0)).groupBy(x => x).
      map(e => (e._2.size,e._1)).countByKey()
    val moretvMap2 = sqlContext.sql("select a.userId from moretv a join sample b on a.userId = b.userId " +
      "where a.date between '2016-03-10' and '2016-03-16'").map(_.getString(0)).groupBy(x => x).
      map(e => (e._2.size,e._1)).countByKey()
    val moretvMap3 = sqlContext.sql("select a.userId from moretv a join sample b on a.userId = b.userId " +
      "where a.date between '2016-03-17' and '2016-03-21'").map(_.getString(0)).groupBy(x => x).
      map(e => (e._2.size,e._1)).countByKey()
    println("=============medusaMap1============")
    medusaMap1.foreach(e => println(e._1,e._2))

    println("=============medusaMap2============")
    medusaMap2.foreach(e => println(e._1,e._2))

    println("=============medusaMap3============")
    medusaMap3.foreach(e => println(e._1,e._2))

    println("=============moretvMap1============")
    moretvMap1.foreach(e => println(e._1,e._2))

    println("=============moretvMap2============")
    moretvMap2.foreach(e => println(e._1,e._2))

    println("=============moretvMap3============")
    moretvMap3.foreach(e => println(e._1,e._2))



  }

}
