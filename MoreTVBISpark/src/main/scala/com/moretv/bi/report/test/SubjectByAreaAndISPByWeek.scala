package com.moretv.bi.report.test

import java.lang.{Long => JLong}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.report.medusa.tempStatistic.MedusaplayInfoModel._
import com.moretv.bi.util.IPLocationUtils.{IPLocationDataUtil, IPOperatorsUtil}
import com.moretv.bi.util.{DFUtil, DateFormatUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by 王宝志 on 2016/9/12.
  * 最近半年各地区各运营商每周播放各专题内节目的人数和次数趋势（只需统计，无需展示）
  *
  * /log/medusaAndMoretvMerger/20160911/playview/ ,历史原因，20160911记录的是20160910当天的数据
  *
  */
object SubjectByAreaAndISPByWeek {
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100").setAppName("SubjectByAreaAndISPByWeek-wangbaozhi")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
        sqlContext.udf.register("getISP",IPOperatorsUtil.getISPInfo _)
        //val util = new DBOperationUtils("medusa")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i=>{
          /**
            * 当date为周一时计算上一周的数据，历史原因，20160912为周一，但目录存储的是周日的数据
            *
            */
          if(DateFormatUtils.isMonday(cal)){
            val date = DateFormatUtils.readFormat.format(cal.getTime)
            val week = DateFormatUtils.getWeekCN(cal)
            println("-----week="+week)

            DFUtil.getDFByDateV3("playview",date,7,sqlContext).select("userId","pathSpecial","ip").registerTempTable("log")

            //按各地区、专题维度查询
            val userByArea = sqlContext.sql("select getProvince(ip),substr(pathSpecial,9) as subject,count(distinct userId),count(userId) from log where pathSpecial like 'subject%' group by getProvince(ip),substr(pathSpecial,9) ").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3)))
            println(week+",userByArea:")
            userByArea.collect().foreach(println)

            //按各地区、专题维度查询
            val userByISP = sqlContext.sql("select getISP(ip),substr(pathSpecial,9) as subject,count(distinct userId),count(userId) from log where pathSpecial like 'subject%' group by getISP(ip),substr(pathSpecial,9) ").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3)))
            println(week+",userByISP:")
            userByISP.collect().foreach(println)

            //按各地区、各运营商、专题维度查询
            val userByAreaAndISP = sqlContext.sql("select getProvince(ip),getISP(ip),substr(pathSpecial,9) as subject,count(distinct userId),count(userId) from log where pathSpecial like 'subject%' group by getProvince(ip),getISP(ip),substr(pathSpecial,9) ").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3),e.getLong(4)))
            println(week+",userByAreaAndISP:")
            userByAreaAndISP.collect().foreach(println)
          }

          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }

  }

}


/**
  *
  *
       nohup /hadoopecosystem/spark/bin/spark-submit --master spark://10.10.2.14:7077  \
       --conf "spark.executor.extraJavaOptions=-XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
       -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70 \
       -XX:+CMSParallelRemarkEnabled"   \
       --class com.moretv.bi.report.test.SubjectByAreaAndISPByWeek ./MoreTVBISpark.jar  \
       --startDate 20160912 --numOfDays 14 >result_1.log 2>&1 &

  *
  *
  *
  */
