package com.moretv.bi.temp.user

import com.moretv.bi.util.FileUtils._
//import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import  com.moretv.bi.util.IPUtils


/**
 * Created by zhangyu on 2016/7/22.
  * 统计时间区间内的新增用户数的城市分布
  * 程序中时间区间已固定，暂不修改。
 */

object AddUserBasedCity extends SparkSetting {
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

     sqlContext.read.load("/log/dbsnapshot/parquet/20160721/" +
       "moretv_mtv_account").registerTempTable("log_data")
//
//    val reslist = sqlContext.sql("select getCity(ip),ip from log_data " +
//      "where openTime between '2016-06-01 00:00:00' and '2016-06-30 23:59:59'").
//      collectAsList()
//    val filepath = s"/script/bi/moretv/zhangyu/file/unknownIP.csv"
//
//    withCsvWriterOld(filepath) {
//      out => {
//        reslist.foreach(x => {
//          if (x.getString(0).equals("未知"))
//            out.println(x.getString(1))
//        })
//      }
//    }
//  }
//}

    val start = Array("2016-06-01","2016-05-01","2016-04-01","2016-03-01",
      "2016-02-01","2016-01-01","2015-12-01","2015-11-01","2015-10-01","2015-09-01","2015-08-01",
    "2015-07-01","2015-06-01")

    val end = Array("2016-06-30","2016-05-31","2016-04-30","2016-03-31",
    "2016-02-29","2016-01-31","2015-12-31","2015-11-30","2015-10-31","2015-09-30",
    "2015-08-31","2015-07-31","2015-06-30")

    val filePrefix = Array("201606","201605","201604","201603","201602","201601",
    "201512","201511","201510","201509","201508","201507","201506")

    (0 until 13).foreach(i=>{
      val startTime = s"${start(i)}" + " " + "00:00:00"
      val endTime = s"${end(i)}" + " " + "23:59:59"
      val fileTime = filePrefix(i)
      //println(s"$startTime , $endTime , $fileTime")

      val rdd = sqlContext.sql(s"select getProvince(ip),getCity(ip), count(distinct mac) from log_data " +
        s"where openTime between '$startTime' and '$endTime' " +
        s"group by getProvince(ip),getCity(ip)").
        map(row =>((row.getString(0),row.getString(1)),row.getLong(2))).collectAsMap()
      val filepath = s"/script/bi/moretv/zhangyu/file/allCityAdduser$fileTime.csv"

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



