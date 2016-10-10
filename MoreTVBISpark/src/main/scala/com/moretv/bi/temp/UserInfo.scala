package com.moretv.bi.temp

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.moretv.bi.util.FileUtils._

/**
  * Created by Will on 2016/7/26.
  */
object UserInfo extends SparkSetting{

  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    val list = sqlContext.read.load("/log/whaley/parquet/*/userinfor").filter("productSN = ''").map(row => {
      val cpuID = row.getString(2)
      val datetime = row.getString(4)
      val mac = row.getString(25)
      val str = row.toString
      val length = str.length
      val log = str.substring(1,length - 1)
      if(cpuID != null) {
        if(cpuID != "") {
          (datetime,"cpuID-"+cpuID,log)
        }else if(mac != null && mac != "") (datetime,"mac-"+mac,log) else (datetime,"other",log)
      }else (datetime,"cpuID-null",log)
    }).collect().toList.sortBy(_._1)
    import scala.collection.mutable
    val map = new mutable.HashMap[String,String]()
    list.foreach(x => {
      if(!map.contains(x._2)) map += x._2 -> x._3
    })
    withCsvWriterOld(s"/script/bi/moretv/liankai/file/userinfo.csv"){
      out => {

        map.foreach(e => {
          out.println(e._1,e._2)
        })
      }
    }
  }
}
