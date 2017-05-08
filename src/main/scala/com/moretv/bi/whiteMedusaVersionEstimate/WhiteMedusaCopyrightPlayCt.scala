package com.moretv.bi.whiteMedusaVersionEstimate

import java.sql.{DriverManager, Statement}
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions.{col, udf}

/**
  * Created by zhu.bingxin on 2017/5/3.
  * 统计
  * 1、白猫版本有版权点播播放人数、次数、播放时长
  * 2、白猫版本有版权点播分栏目播放人数、次数、播放时长
  * 3、白猫版本有版权点播集体节目播放人数、次数、播放时长
  * 统计区间：每天
  * 统计维度：day
  * 统计度量：playNum，playUser，playSumDuration
  * 限制条件：有版权（videoSource in qq，tencent2，copyright = 1）
  * event = 'startplay' （统计播放量）
  * event != 'startplay' and duration BETWEEN 0 AND 10800（统计播放时长）
  */
object WhiteMedusaCopyrightPlayCt extends BaseClass {

  private val table = "whiteMedusa_copyright_play_ct"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  override def execute(args: Array[String]): Unit = {

    /**
      * UDF
      */
    val udfToDateCN = udf { yyyyMMdd: String => DateFormatUtils.toDateCN(yyyyMMdd) }
    sqlContext.udf.register("getApkVersion", getApkVersion _)
    sqlContext.udf.register("getVersion", getVersion _)

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val numOfPartition = 100

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)

        val sqlContextTemp = sqlContext
        import sqlContextTemp.implicits._

        val sqlData = s"SELECT sid FROM `mtv_basecontent` where  id >= ? and id <= ? and copyright = 1"
        val sqlMinMaxId = "select MIN(id),MAX(id) from mtv_basecontent"
        val sidDF = MySqlOps.getJdbcRDD(sc, DataBases.MORETV_CMS_MYSQL, sqlMinMaxId, sqlData, numOfPartition, r => {
          r.getString(1)
        })
          .toDF("sid")
          .distinct()
          .cache()
        sidDF.registerTempTable("sid")

        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION)
          .select("version")
          .distinct()
          .cache()
          .registerTempTable("app_version_log")
        (0 until p.numOfDays).foreach(i => {

          //define the day
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)

          //define database
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

          //add 1 day after read current day
          cal.add(Calendar.DAY_OF_MONTH, 1)
          val date2 = DateFormatUtils.readFormat.format(cal.getTime)

          //load data
          val logPlay = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY, date2)
            .filter("videoSource like '%qq%' or videoSource like '%tencent2%'")
          logPlay.registerTempTable("play")

          //filter data
          sqlContext.sql(
            """
              |select a.apkVersion,a.contentType,a.videoSid,a.userId,a.duration,a.event
              |from play a join sid b
              |on a.videoSid = b.sid
            """.stripMargin)
            .registerTempTable("tempTable1") //只保留有版权的sid内容

          sqlContext.sql(
            """
              |select a.userId,b.version,a.contentType,a.videoSid,a.duration,a.event
              |from tempTable1 a
              |left join app_version_log b
              |on getApkVersion(a.apkVersion) = b.version
            """.stripMargin)
            .registerTempTable("tempTable2") //只保留正常版本的sid内容

          //data processings
          sqlContext.sql(
            """
              |select count(userId) as playNum,count(distinct userId) as playUser
              |from
              |tempTable2
              |where version >= '3.1.4' and event = 'startplay'
            """.stripMargin)
            .registerTempTable("result1") //得到播放次数和人数

          sqlContext.sql(
            """
              |select sum(duration) as playSumDuration
              |from
              |tempTable2
              |where version >= '3.1.4' and event != 'startplay' and duration between 0 and 10800
            """.stripMargin)
            .registerTempTable("result2") //得到播放总时长

          val resultDf = sqlContext.sql(
            """
              |select a.playNum,a.playUser,b.playSumDuration
              |from result1  a join result2 b
            """.stripMargin)

          val insertSql = s"insert into $table(day,playNum,playUser,playSumDuration) " +
            "values (?,?,?,?)"
          if (p.deleteOld) {
            val deleteSql = s"delete from $table where day=?"
            util.delete(deleteSql, insertDate)
          }

          //          sqlContext.sql("select * from play limit 10").show(false)
          //          sqlContext.sql("select * from tempTable1 limit 10").show(false)
          //          sqlContext.sql("select * from tempTable2 limit 10").show(false)
          //          sqlContext.sql("select * from result1 limit 10").show(false)
          //          sqlContext.sql("select * from result2 limit 10").show(false)
          //          sqlContext.sql("select * from play limit 10").show(false)
          //          sqlContext.sql("select * from play limit 10").show(false)
          //          resultDf.show(false)

          resultDf.collect.foreach(e => {
            util.insert(insertSql, insertDate, e.get(0), e.get(1), e.get(2))
          })
          println(insertDate + " Insert data successed!")
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }


  /**
    * 从apkSerial中提取出apkVersion
    *
    * @param apkSerials
    * @return
    */
  def getApkVersion(apkSerials: String) = {
    if (apkSerials != null) {
      if (apkSerials == "")
        "kong"
      else if (apkSerials.contains("_")) {
        apkSerials.substring(apkSerials.lastIndexOf("_") + 1)
      } else {
        apkSerials
      }
    } else
      "null"
  }

  /**
    * 将version新旧版区分开
    */
  def getVersion(apkVersion: String) = {
    if (apkVersion != null && apkVersion >= "3.1.4") "new"
    else "old"
  }


  def getID(stmt: Statement): Array[Long] = {
    val sql = s"SELECT MIN(id),MAX(id) FROM mtv_cms.`mtv_basecontent`"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1), id.getLong(2))
  }
}
