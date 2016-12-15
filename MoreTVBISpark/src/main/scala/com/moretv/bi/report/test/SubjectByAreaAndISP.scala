package com.moretv.bi.report.test

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.tempStatistic.MedusaplayInfoModel._
import com.moretv.bi.util.IPLocationUtils.{IPLocationDataUtil, IPOperatorsUtil}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by 王宝志 on 2016/9/12.
  * 最近半年各地区各运营商每周播放各专题内节目的人数和次数趋势（只需统计，无需展示）
  */
object SubjectByAreaAndISP {
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100").setAppName("SubjectByAreaAndISP-wangbaozhi")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
        sqlContext.udf.register("getISP",IPOperatorsUtil.getISPInfo _)
        val medusaDir = "/log/medusaAndMoretvMerger/"
        //val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"$medusaDir/$dateTime/playview/"
          println("-----inputPath is "+inputPath)
          //val day = DateFormatUtils.toDateCN(dateTime,-1)
          sqlContext.read.load(inputPath).select("userId","pathSpecial","ip").registerTempTable("log")

          //查询
          val userByAreaAndISP = sqlContext.sql("select getProvince(ip),getISP(ip),substr(pathSpecial,9) as subject,count(distinct userId),count(userId) from log where pathSpecial like 'subject%' group by getProvince(ip),getISP(ip),substr(pathSpecial,9) ").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3),e.getLong(4)))

          //打印出结果
          userByAreaAndISP.collect().foreach(println)
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }

  }

}
