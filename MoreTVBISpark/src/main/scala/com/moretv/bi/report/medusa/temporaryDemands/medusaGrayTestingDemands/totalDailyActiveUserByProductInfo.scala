package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 每个终端的日活用户
 */
object totalDailyActiveUserByProductInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate


        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaDailyActiveInput = s"$medusaDir/$date/*/"
          val moretvDailyActiveInput = s"$moretvDir/*/$date"

           sqlContext.read.parquet(medusaDailyActiveInput).select("userId","apkVersion","productModel").
             registerTempTable("medusa_daily_active_log")
          sqlContext.read.parquet(moretvDailyActiveInput).select("userId","apkVersion","productModel").
            registerTempTable("moretv_daily_active_log")

          val totalActiveUser=sqlContext.sql("select productModel,count(distinct a.userId) from (select productModel, " +
            "userId from " +
            "medusa_daily_active_log Union select productModel, userId from moretv_daily_active_log) as a group by " +
            "productModel").map(e=>(e.getString(0),e.getLong(1)))

          val sqlInsert = "insert into medusa_total_active_user_by_product_model_info(day,product_model,active_user) " +
            "values (?,?,?)"

          totalActiveUser.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,new JLong(e._2))
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
