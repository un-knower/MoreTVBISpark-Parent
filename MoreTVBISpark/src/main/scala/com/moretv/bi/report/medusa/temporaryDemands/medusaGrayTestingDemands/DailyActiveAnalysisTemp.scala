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
object DailyActiveAnalysisTemp extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate


        val medusaDir = "/mbi/parquet"
        val accountDir = "/log/dbsnapshot/parquet"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaEnterInput = s"$medusaDir/*/$date"

          sqlContext.read.parquet(medusaEnterInput).select("userId").registerTempTable("log1")
          val medusaActiveUserRdd = sqlContext.sql("select count(distinct userId) from log1")
            .map(e=>(insertDate,e.getLong(0)))


          val accountInput = s"$accountDir/20160718/moretv_mtv_account"
          sqlContext.read.parquet(accountInput).select("user_id","openTime","userType").registerTempTable("log2")
          val limitTime = s"$insertDate"+" "+"23:59:59"
          val accountUserRdd = sqlContext.sql(s"select count(distinct user_id) from log2 where substring(openTime,0,11)" +
            s"<'$limitTime' and userType !=1").map(e=>(insertDate,e.getLong(0)))

          val merger = medusaActiveUserRdd.join(accountUserRdd).collect()



          val sqlInsert = "insert into medusa_daily_active_analysis_temp(day,active_user,account_user) values (?,?,?)"
          merger.foreach(e=>{
            util.insert(sqlInsert,insertDate,new JLong(e._2._1),new JLong(e._2._2))
          })




        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
