package com.moretv.bi.temp.liveGradationTestData

import java.sql.{DriverManager, Statement}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/10/11.
 * 计算小鹰直播10月8号和9号升级至3.1.1版本的用户,每日的直播频道播放情况
 */
object GradationUsersDailyInfo extends SparkSetting {
  private val tableName = "medusa_live_gradation_test_auc_info"
  private val deleteSql = s"delete from ${tableName} where day=?"
  private val insertSql = s"insert into ${tableName}(day,user) values (?,?)"
  val util = DataIO.getMySqlOps("medusa_mysql")

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val gradationUsersDir = "/log/medusa/temple/gradationUsers"
        val parentDir = "/log/medusa/parquet/"
        val logType = "*"
        sqlContext.read.parquet(gradationUsersDir).registerTempTable("log_gradation")
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)
          val date = s"${logDay}/"
          val inputDir = s"${parentDir}${date}${logType}"
          sqlContext.read.parquet(inputDir).select("userId").registerTempTable("log_all")

          /**
           * 计算灰度升级用户的直播次数与直播人数
           */
          val userRdd = sqlContext.sql(
            """select count(distinct a.userId)
              |from log_all as a join log_gradation as b on a.userId=b.userId
              |""".stripMargin).
            map(e => e.getLong(0)).collect()

          if(p.deleteOld){
            util.delete(deleteSql,sqlDay)
          }
          userRdd.foreach(e=>{
            util.insert(insertSql,sqlDay,e)
          })

          calendar.add(Calendar.DAY_OF_MONTH,-1)
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
