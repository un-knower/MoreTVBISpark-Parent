package com.moretv.bi.temp

import java.io.{File, PrintWriter}
import java.util.regex.Pattern

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Will on 2015/2/5.
 */
object LoginHttpStatusDistribution extends SparkSetting{

  val pattern = Pattern.compile("/(login|openApi)/Service/(logon|login|enlogin)\\?.?mac=([a-zA-Z0-9]{12}).+?\" (\\d{3}) ")

  def main(args: Array[String]) {

    config.setAppName("LoginHttpStatusDistribution-登录日志的HTTP状态码分布")
    val sc = new SparkContext(config)

    val logRDD = sc.textFile("/log/loginlog/loginlog.access.log_"+args(0)+"*").map(line => matchLog(line)).filter(_ != null).persist()

    val times = logRDD.countByKey()
    val user = logRDD.distinct().countByKey()

    println("#########################################")
    times.foreach(x => {
      System.out.println("accessTimes:\t"+x._1 + "\t" + x._2)
    })
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    user.foreach(x => {
      System.out.println("accessUser:\t"+x._1 + "\t" + x._2)
    })
    println("#########################################")
    logRDD.unpersist()
  }

  def matchLog(log:String) ={
    val matcher = pattern.matcher(log)
    if(matcher.find()){
      (matcher.group(4),matcher.group(3))
    }else null
  }

}
