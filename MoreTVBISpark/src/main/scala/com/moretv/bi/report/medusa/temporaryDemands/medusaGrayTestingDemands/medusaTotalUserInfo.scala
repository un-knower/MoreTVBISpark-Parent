package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/23.
 */
object medusaTotalUserInfo extends SparkSetting{

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate

        val medusaDir = "/log/medusa/parquet"
        val enterLogType = "enter"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        val insertDate = DateFormatUtils.toDateCN(DateFormatUtils.readFormat.format(calendar.getTime),-1)


        val inputs = new Array[String](p.numOfDays)
        for(i <- 0 until p.numOfDays){
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          inputs(i) = s"$medusaDir/$date/$enterLogType/"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
        val medusaEnterlog = sqlContext.read.parquet(inputs:_*)
        medusaEnterlog.select("userId","apkVersion","buildDate").registerTempTable("medusa_total_log")
        val medusaTotalUserRdd = sqlContext.sql("select count(distinct userId) from medusa_total_log where apkVersion='3.0" +
          ".5' and buildDate='20160518'").map(e=>e.getLong(0)).collect()

        val insertSql = "insert into medusa_gray_testing_total_user_each_day(date,buildDate," +
          "apk_version,user_num) values (?,?,?,?)"

        medusaTotalUserRdd.foreach(r=>{
          util.insert(insertSql,insertDate,"20160518","3.0.6",new JLong(r))
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
