package com.moretv.bi.user

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{QueryMaxAndMinIDUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

import com.moretv.bi.util.FileUtils._


/**
 * Created by Will on 2015/4/18.
 */
object UserHoroscopeStatistics extends BaseClass with QueryMaxAndMinIDUtil{
  def main(args: Array[String]) {
    config.setAppName("UserHoroscopeStatistics")
    ModuleClass.executor(UserHoroscopeStatistics,args)
  }
  override def execute(args: Array[String]) {
    val yesterday = Calendar.getInstance()
    val formatCN = new SimpleDateFormat("yyyy-MM-dd")
    val count = Integer.parseInt(args(0))
    yesterday.add(Calendar.DAY_OF_MONTH, -count)
    val yesterdayCN = formatCN.format(yesterday.getTime)
    val id = queryID("uid","bbs_ucenter_memberfields","jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

    val resultRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
    },
      "SELECT IFNULL(birthday,'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ?",
      id(1), id(0), 10,
      r=>r.getString(1)).map(e=>(matchLog(e))).filter(_!=null).countByKey()

    val path = "/home/moretv/sparkTest/user_horoscope_"+yesterdayCN+".txt"
    withCsvWriterOld(path){
      out =>{
        resultRDD.foreach(e =>{
          out.println(yesterdayCN +"  "+e._1+"  "+e._2)
        })
      }
    }
  }

  def matchLog(log:String) = {
    if (log.equalsIgnoreCase("")){
      ("未知",1)
    }else if(log.contains("月") && log.contains("日")){
      val month = log.substring(log.indexOf('年')+1,log.indexOf('月'));
      val day   = log.substring(log.indexOf('月')+1,log.indexOf('日'));
      try{
        val horoscop = getHoroscopeName(month.toInt,day.toInt);
        (horoscop,1)
      }catch {
        case e:Exception => {null}
      }
    }else null
  }

  def getHoroscopeName(month:Int , day:Int):String={
    var star = "";
    if (month == 1 && day >= 20 || month == 2 && day <= 18) {
      star = "水瓶座";
    }
    if (month == 2 && day >= 19 || month == 3 && day <= 20) {
      star = "双鱼座";
    }
    if (month == 3 && day >= 21 || month == 4 && day <= 19) {
      star = "白羊座";
    }
    if (month == 4 && day >= 20 || month == 5 && day <= 20) {
      star = "金牛座";
    }
    if (month == 5 && day >= 21 || month == 6 && day <= 21) {
      star = "双子座";
    }
    if (month == 6 && day >= 22 || month == 7 && day <= 22) {
      star = "巨蟹座";
    }
    if (month == 7 && day >= 23 || month == 8 && day <= 22) {
      star = "狮子座";
    }
    if (month == 8 && day >= 23 || month == 9 && day <= 22) {
      star = "处女座";
    }
    if (month == 9 && day >= 23 || month == 10 && day <= 22) {
      star = "天秤座";
    }
    if (month == 10 && day >= 23 || month == 11 && day <= 21) {
      star = "天蝎座";
    }
    if (month == 11 && day >= 22 || month == 12 && day <= 21) {
      star = "射手座";
    }
    if (month == 12 && day >= 22 || month == 1 && day <= 19) {
      star = "摩羯座";
    }
    star;
  }
}
