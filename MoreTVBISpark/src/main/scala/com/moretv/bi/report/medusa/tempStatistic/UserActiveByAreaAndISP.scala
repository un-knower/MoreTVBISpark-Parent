package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.tempStatistic.MedusaplayInfoModel._
import com.moretv.bi.util.IPLocationUtils.{IPOperatorsUtil, IPLocationDataUtil}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/4/26.
 */
object UserActiveByAreaAndISP {
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
        sqlContext.udf.register("getISP",IPOperatorsUtil.getISPInfo _)
        val util = new DBOperationUtils("medusa")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/moretvloginlog/parquet/$dateTime/loginlog"
          val day = DateFormatUtils.toDateCN(dateTime,-1)
          sqlContext.read.load(inputPath).select("ip","mac").registerTempTable("log")
          val userByArea = sqlContext.sql("select getProvince(ip), count(distinct mac) from log group by getProvince(ip)").
            map(e=>(e.getString(0),e.getLong(1)))
          val userByISP = sqlContext.sql("select getISP(ip), count(distinct mac) from log group by getISP(ip)").
            map(e=>(e.getString(0),e.getLong(1)))
          val userByAreaAndISP = sqlContext.sql("select getProvince(ip),getISP(ip), count(distinct mac) from log group by getProvince(ip),getISP(ip)").
            map(e=>(e.getString(0),e.getString(1),e.getLong(2)))

          val insertSql = "insert into daily_active_user_by_area_isp(day,province,isp,user) values(?,?,?,?)"
          userByArea.collect().foreach(e=>{
            util.insert(insertSql,day,e._1,"all",new JLong(e._2))
          })
          userByISP.collect().foreach(e=>{
            util.insert(insertSql,day,"all",e._1,new JLong(e._2))
          })
          userByAreaAndISP.collect().foreach(e=>{
            util.insert(insertSql,day,e._1,e._2,new JLong(e._3))
          })


          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }

  }

}
