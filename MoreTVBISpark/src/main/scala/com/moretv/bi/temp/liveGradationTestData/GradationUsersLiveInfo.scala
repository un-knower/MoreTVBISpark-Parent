package com.moretv.bi.temp.liveGradationTestData

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/10/11.
 * 计算小鹰直播10月8号和9号升级至3.1.1版本的用户,每日的直播频道播放情况
 */
object GradationUsersLiveInfo extends SparkSetting{
  private val tableName = "medusa_live_gradation_test_live_info"
  private val deleteSql = s"delete from ${tableName} where day=?"
  private val insertSql =
    s"""insert into ${tableName}(day,gradation_user,live_user,live_num,live_duration)
       |values (?,?,?,?,?)
     """.stripMargin
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps("medusa_mysql")
        val gradationUsersDir = "/log/medusa/temple/gradationUsers/*"
        val parentDir = "/log/medusa/parquet/"
        val logType = "live"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)
          val date = s"${logDay}/"
          val inputDir = s"${parentDir}${date}${logType}"
          sqlContext.read.parquet(gradationUsersDir).registerTempTable("log_gradation")
          sqlContext.read.parquet(inputDir).select("userId","event","duration",
            "liveType","apkVersion").filter("liveType='live'").
            registerTempTable("log_live")

          /**
           * 计算灰度升级用户量
           */
          val gradationUserNum = sqlContext.sql(
            """select count(distinct userId)
              |from log_gradation""".stripMargin).map(e=>e.getLong(0)).collect
          /**
           * 计算灰度升级用户的直播次数与直播人数
           */
          val userDF = sqlContext.sql(
            """select count(a.userId),count(distinct a.userId)
              |from log_live as a join log_gradation as b on a.userId=b.userId
              |where a.event='startplay' and a.liveType = 'live'""".stripMargin).
            map(e=>(e.getLong(0),e.getLong(1))).collect()
          /**
           * 计算灰度升级用户的直播时长，单位为s
           */
          val durationDF = sqlContext.sql(
            """select sum(a.duration)
              |from log_live as a join log_gradation as b on a.userId=b.userId
              |where a.event='switchchannel' and a.duration between 0 and 36000""".stripMargin).
            map(e=>e.getLong(0)).collect()

          /**
           * 与数据库交互
           */
          if(p.deleteOld){
            util.delete(deleteSql,sqlDay)
          }
          userDF.foreach(e=>{
            util.insert(insertSql,sqlDay,gradationUserNum(0),e._1,e._2,durationDF(0))
          })
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
