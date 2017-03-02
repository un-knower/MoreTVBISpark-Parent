package com.moretv.bi.report.medusa.dataAnalytics

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.dataAnalytics.DataAnalyticsPlayDistribution._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by michael on 3/2/17.
  *
  * 统计用户观影时常分布(同一个用户观看同一个影片，总时间长度超过3个小时的时间分布)
  *
  */

object DataAnalyticsPlayDurationDistribution extends BaseClass {

  private val tableName = "data_analytic_play_duration_distribution"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(DataAnalyticsPlayDistribution, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance
        sqlContext.udf.register("getVersion",getVersion _)
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          if (p.deleteOld) {
            val deleteSql = s"delete from $tableName where day = ?"
            util.delete(deleteSql, insertDate)
          }
          val insertSql = s"insert into $tableName(day,content_type,version,duration,play_user,total_user) values(?,?,?,?,?,?)"
          //临时没有该parquet文件,容错
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW).select("userId", "videoSid", "contentType",
            "event", "apkVersion","duration").unionAll(
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY).select("userId", "videoSid", "contentType",
              "event", "apkVersion","duration")).registerTempTable("log_data")

          /*
            medusa退出play的event类型： 'selfend','userexit'，helios退出play的event类型：'playview'
            duration>10800 [秒，表示三个小时]
           */

          /*统计在version,userId,videoSid纬度上，观看同一个影片时常超过3个小时的记录 */
          sqlContext.sql(
            """
              |select contentType,getVersion(apkVersion) as version,userId,videoSid,sum(duration) as duration_sum
              |from log_data
              |where event in ('selfend','userexit','playview')
              |group by contentType,getVersion(apkVersion) as version,userId,videoSid
              |having duration_sum>10800
            """.stripMargin).registerTempTable("log_play")

          /*统计在不同观看时常纬度上的用户数量分布*/
          sqlContext.sql(
            """
              |select contentType,version,duration_sum,count(userId) as playUser
              |from log_play
              |group by contentType,version,duration_sum
            """.stripMargin).registerTempTable("log_play_distribution")

          /*统计在不同观看时常纬度上的用户数量分布，附带特定duration_sum，特定playUser的总用户数信息*/
          sqlContext.sql(
            """
              |select a.contentType,a.version,a.duration_sum,a.playUser,b.totalNum
              |from log_play_distribution as a
              |join
              |(
              |select contentType,version,count(playUser) as totalUser
              |from log_play_distribution
              |group by contentType,version
              |) as b
              |on a.contentType=b.contentType and a.version = b.version
            """.stripMargin).foreachPartition(partition => {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(rdd => {
              util1.insert(insertSql, insertDate, rdd.getString(0), rdd.getString(1), rdd.getLong(2), rdd.getLong(3),rdd.getLong(4))
            })
          })
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

  /**
    * Get the version info
    *
    * @param apkVersion
    * @return
    */
  def getVersion(apkVersion:String): String ={
    if(apkVersion!=null){
      apkVersion.substring(0,1)
    }else{
      "未知"
    }
  }
}

/*
2-15 machine
* use medusa;
CREATE TABLE `data_analytic_play_duration_distribution` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `day` varchar(20) NOT NULL DEFAULT '',
  `version` varchar(2) NOT NULL DEFAULT '' COMMENT '版本信息：2/3',
  `content_type` varchar(20) NOT NULL DEFAULT '' COMMENT '频道信息',
  `duration` bigint(40) NOT NULL DEFAULT '0' COMMENT '单用户单节目播放时长',
  `play_user` bigint(40) NOT NULL DEFAULT '0' COMMENT '单用户单节目播放量所对应的人数',
  `total_user` bigint(40) NOT NULL DEFAULT '0' COMMENT '总人数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
* */