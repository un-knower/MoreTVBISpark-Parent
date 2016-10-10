package com.moretv.bi.temp.user

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.DateFormatUtils
import com.moretv.bi.util.FileUtils._
//import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.{IPUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Created by huanghu on 2016/7/25.
  * 统计时间区间内的活跃用户数的城市分布
  * 程序中时间区间已固定，暂不修改。
 */

object AcctiveUserBasedCity extends SparkSetting {
  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)

    //sqlContext.udf.register("getCity", IPLocationDataUtil.getCity _)
    //sqlContext.udf.register("getProvince", IPLocationDataUtil.getProvince _)

    sqlContext.udf.register("getCity",(ip:String) => {
      val result = IPUtils.getProvinceAndCityByIp(ip)
      if(result != null) result(1) else "未知"
    })
    sqlContext.udf.register("getProvince",IPUtils.getProvinceByIp _)

    val filePrefix = Array("201606","201605","201604","201603","201602","201601",
    "201512","201511","201510","201509","201508","201507","201506")

   // val filePrefixBegin = Array("20160601","20160501","20160401","20160301","20160201","20160101",
     // "20151201","20151101","20151001","20150901","20150801","20150701","20150601")

    val filePrefixEnd= Array("20160701","20160601","20160501","20160401","20160301","20160201",
      "20160101","20151201","20151101","20151001","20150901","20150801","20150701")

    val cal = Calendar.getInstance()
    cal.set(Calendar.MONTH,5)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    (0 until 13).foreach(i=>{
      val min = cal.getActualMinimum(Calendar.DAY_OF_WEEK)
      cal.set(Calendar.DAY_OF_MONTH,min)
      val begin = format.format(cal.getTime)
      val max = cal.getActualMaximum(Calendar.DAY_OF_MONTH)
      cal.set(Calendar.DAY_OF_MONTH,max)
      val end = format.format(cal.getTime)
      cal.add(Calendar.MONTH,-1)
      val fileTime = filePrefix(i)
      //val fileTimeBegin = filePrefixBegin (i)
      val fileTimeEnd = filePrefixEnd (i)

      sqlContext.read.load(s"/log/moretvloginlog/parquet/$fileTime*/" +
        "/loginlog").select("ip","date","mac").unionAll(sqlContext.read.load(s"/log/moretvloginlog/parquet/$fileTimeEnd/" +
        "/loginlog").select("ip","date","mac")).registerTempTable("log_data")


      //val rdd = sqlContext.sql(s"select getProvince(ip),getCity(ip), count(distinct mac) from log_data " +
        //s"group by getProvince(ip),getCity(ip)").map(row =>((row.getString(0),row.getString(1)),row.getLong(2))).collectAsMap()

      val rdd = sqlContext.sql(s"select getProvince(ip),getCity(ip), count(distinct mac) from log_data " +
        s" where date between '$begin'and '$end' " +
        s"group by getProvince(ip),getCity(ip)").map(row =>((row.getString(0),row.getString(1)),row.getLong(2))).collectAsMap()

      val filepath = s"/script/bi/moretv/huanghu/file/allCityAcctiveuser$fileTime.csv"

      withCsvWriterOld(filepath){
        out => {
          rdd.foreach(x => {
            out.println(x._1._1 + "," + x._1._2 + "," + x._2)
          })
        }
      }

    })
  }
}



