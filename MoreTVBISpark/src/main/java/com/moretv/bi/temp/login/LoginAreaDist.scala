package com.moretv.bi.temp.login

import com.moretv.bi.util.{LogInputUtil, SparkSetting}
import com.moretv.ip.IPUtils
import org.apache.spark.SparkContext
import com.moretv.bi.util.FileUtils._
import java.util.Date

/**
 * Created by Will on 2015/9/12.
 */
object LoginAreaDist extends SparkSetting{

  private val regex = "\"(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})".r
  private val regex2 = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})".r
  private val regexMac = "mac=(\\w{12})".r

  def main(args: Array[String]) {
    val inputDate = args(0)
    val inputPath = LogInputUtil.getLoginLog(inputDate)
    val sc = new SparkContext(config)
    val rdd = sc.textFile(inputPath).map(matchLog).filter(_ != null).cache()

    val loginNumMap = rdd.map(_._2).countByValue()
    val loginUserMap = rdd.distinct().map(_._2).countByValue()

    withCsvWriter("LoginAreaDist_"+inputDate+".csv"){
      out => {
        out.println(new Date)
        loginNumMap.foreach(e => {
          val area = e._1
          val userNum = loginUserMap(area)
          out.println(area+","+userNum+","+e._2)
        })
      }
    }
  }

  def matchLog(log:String) = {
    val ip = regex findFirstMatchIn log match {
      case Some(m) => IPUtils.getProvinceByIp(m.group(1))
      case None => {
        regex2 findFirstMatchIn log match {
          case Some(m) => IPUtils.getProvinceByIp(m.group(1))
          case None => null
        }
      }
    }
    val mac = regexMac findFirstMatchIn log match {
      case Some(m) => m.group(1)
      case None => null
    }
    if(mac != null && ip != null) (mac,ip) else null
  }
}
