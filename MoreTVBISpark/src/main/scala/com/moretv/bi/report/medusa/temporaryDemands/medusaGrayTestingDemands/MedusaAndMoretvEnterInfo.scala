package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MedusaAndMoretvEnterInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate


        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"
        val enterLogType = "enter"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaEnterInput = s"$medusaDir/$date/$enterLogType/"
          val moretvEnterInput = s"$moretvDir/$enterLogType/$date/"

          sqlContext.read.parquet(medusaEnterInput).registerTempTable("medusa_enter_log")
          sqlContext.read.parquet(moretvEnterInput).registerTempTable("moretv_enter_log")


          val medusaEnterNum = sqlContext.sql("select userId from medusa_enter_log")
            .map(e=>e.getString(0)).count()
          val moretvEnterNum = sqlContext.sql("select userId from moretv_enter_log")
            .map(e=>e.getString(0)).count()
          val enterNum=medusaEnterNum+moretvEnterNum



          val sqlInsert = "insert into medusa_gray_testing_denglu_num_total(day,enter_num) values (?,?)"

            util.insert(sqlInsert,insertDate,new JLong(enterNum))


        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
