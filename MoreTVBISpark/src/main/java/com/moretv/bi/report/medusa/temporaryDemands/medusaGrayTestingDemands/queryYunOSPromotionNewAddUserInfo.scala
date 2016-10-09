package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 统计YUNOS的新增用户数
 */
object QueryYunOSPromotionNewAddUserInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate


        val medusaDir = "/log/dbsnapshot/parquet"
        val enterLogType = "moretv_mtv_account"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)

          val medusaEnterInput = s"$medusaDir/$date/$enterLogType/"

          val medusaEnterlog = sqlContext.read.parquet(medusaEnterInput)
          val startTime = s"$insertDate"+" "+"00:00:00"
          val endTime = s"$insertDate"+" "+"23:59:59"

          medusaEnterlog.select("mac","openTime","promotion_channel","current_version").registerTempTable("log_data")

          val rdd = sqlContext.sql(s"select promotion_channel,count(distinct mac) from log_data " +
            s"where openTime between '$startTime' and '$endTime' and current_version like '%YunOS%'" +
            s" group by promotion_channel")
            .map(e=>(e.getString(0),e.getLong(1)))



          val sqlInsert = "insert into mtv_promotion_channel_new_add_user(day,version,promotion_channel,user_num) values " +
            "(?,?,?,?)"

          if(p.deleteOld){
            val deleteSql = "delete from mtv_promotion_channel_new_add_user where day=?"
            util.delete(deleteSql,insertDate)
          }

          rdd.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,"YunOS",e._1,new JLong(e._2))
          })
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
