package com.moretv.bi.temp.login

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import java.util.Calendar

/**
  * Created by zhangyu on 2016/7/22.
  * 判断登录日志中是否含有ip字段
  */
object BoolLoginlogContainsIp extends SparkSetting{
  def main(args:Array[String]) = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i=> {
          val logDate = DateFormatUtils.readFormat.format(cal.getTime)
          val logPath = s"/log/moretvloginlog/parquet/$logDate/loginlog"
          val flag = sqlContext.read.load(logPath).columns.contains("ip")
          println(s"$logDate ip: $flag")
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }

      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
