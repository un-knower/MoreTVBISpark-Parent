package com.moretv.bi.temp.annual

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.IPLocationUtils.{IPLocationDataUtil, IPOperatorsUtil}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by zhu.bingxin on 3/2/17.
  * 【部门】内容运营部
  * 【业务线】电视猫
  * 【需求目的】直播屏蔽策略数据依据
  * 【需求内容1】电视猫24个城市直播日活、人均直播时长
  * 【需求内容2】电视猫24个城市点击电视猫3.0首页直播板块的人数、次数
  * 24个城市：上海   北京   成都   天津   济南   青岛   太原   东莞   广州   深圳   南京   无锡   苏州   郑州   宁波   杭州   武汉   长沙   厦门   福州   大连   沈阳   重庆   西安
  * 时间区间：2017年1月1日至2017年2月28日每日数据
  * 【需求内容提供时间】2017年3月2日下午
  *
  * 备注：20170101-20170121 城市映射取得是ip字段；20170122-20170219 城市映射取得是remoteIp字段
  */
object LiveDay24CityStatistic extends BaseClass {

  private val tableName = "moretv_live_day_24city_statistic"

  private val fields = "day,city,uv,duration_per_uv,home_access_from_live_uv,home_access_from_live_pv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?)"

  private val citysString = "上海   北京   成都   天津   济南   青岛   太原   东莞   广州   深圳   南京   无锡   苏州   郑州   宁波   杭州   武汉   长沙   厦门   福州   大连   沈阳   重庆   西安"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {

          //define the day
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)

          //define database
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

          //add 1 day after read currrent day
          cal.add(Calendar.DAY_OF_MONTH, 1)

          val getCity = udf((e: String) => IPLocationDataUtil.getCity(e))
          val getIpInfo = udf((forwardedIp: String, remoteIp: String) => IPOperatorsUtil.getIPInfo(forwardedIp, remoteIp))

          //load data
          val live_path = s"/log/medusa/parquet/$date/live"
          val homeaccess_path = s"/log/medusa/parquet/$date/homeaccess"

          sqlContext.read.parquet(live_path)
            .filter($"liveType" === "live")
            .filter($"date".between("2017-01-01", "2017-02-28"))
            .withColumn("ip", getIpInfo($"forwardedIp", $"remoteIp"))
            .select($"event", $"duration", lit(date).as("date"), $"userId", getCity($"ip").as("city"))
            .filter(lit(citysString).contains($"city"))
            .registerTempTable("live_table")

          sqlContext.read.parquet(homeaccess_path)
            .filter($"date".between("2017-01-01", "2017-02-28"))
            .filter($"accessArea" === "live")
            .withColumn("ip", getIpInfo($"forwardedIp", $"remoteIp"))
            .select(lit(date).as("date"), $"userId", getCity($"ip").as("city"))
            .filter(lit(citysString).contains($"city"))
            .registerTempTable("homeaccess_table")

          sqlContext.sql(
            """
              |select event,date,city,count(distinct userId) as uv
              |from live_table
              |where event = 'startplay'
              |group by event,date,city
            """.stripMargin)
            .registerTempTable("uv_result")
          //          sqlContext.sql("select * from uv_result").show(100, false)

          sqlContext.sql(
            """
              |select event,date,city,sum(duration) as sumduration
              |from live_table
              |where event = 'switchchannel' and duration between 1 and 36000
              |group by event,date,city
            """.stripMargin)
            .registerTempTable("sumduration_result")


          sqlContext.sql(
            """
              |select date,city,count(distinct userId) as home_access_from_live_uv,count(userId) as home_access_from_live_pv
              |from homeaccess_table
              |group by date,city
            """.stripMargin)
            .registerTempTable("homeaccess_result")


          val resultDf = sqlContext.sql(
            """
              |select u.date,u.city,uv,round(sumduration/uv,3) as duration_per_uv,home_access_from_live_uv,home_access_from_live_pv
              |from uv_result u join sumduration_result s join homeaccess_result h
              |on u.date = s.date and s.date = h.date and u.city = s.city and s.city = h.city
            """.stripMargin)
            .collect()

          //resultDf.show(100, false)

          resultDf.foreach(w => {
            util.insert(insertSql, w.getString(0), w.getString(1), w.getLong(2), w.getDouble(3), w.getLong(4), w.getLong(5))
          })
          println(insertDate + " Insert data successed!")
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
