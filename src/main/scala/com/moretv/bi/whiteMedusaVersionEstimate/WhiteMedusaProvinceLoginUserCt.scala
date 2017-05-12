package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions.udf

/**
  * Created by zhu.bingxin on 2017/5/4.
  * 统计登录用户数
  * 新版本（3.1.4及以上的为新版本）
  * 每天分省统计
  */
object WhiteMedusaProvinceLoginUserCt extends BaseClass {

  private val table = "white_medusa_province_login_user_ct"

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
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)
        (0 until p.numOfDays).foreach(i => {

          //define the day
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)

          //define database
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

          //add 1 day after read currrent day
          cal.add(Calendar.DAY_OF_MONTH, 1)
          // date2 = DateFormatUtils.readFormat.format(cal.getTime)

          //load data
          //val logAccount = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date)
          val logLogin = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date)
          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
            select("version").distinct().registerTempTable("app_version_log")
          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_TERMINAL_USER)
            .select("mac", "web_location_sk", "dim_invalid_time")
            .distinct().registerTempTable("mac")
          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_WEB_LOCATION)
            .select("web_location_sk", "province")
            .distinct().registerTempTable("area")


          //filter data
          logLogin.select("day", "mac", "version")
            .registerTempTable("login_table")

          //data processings
          sqlContext.sql(
            """
              |select a.mac,b.province
              |from mac a join area b
              |on a.web_location_sk = b.web_location_sk
              |where a.dim_invalid_time is null
            """.stripMargin)
            .registerTempTable("mac_area") //取出mac和地区映射表

          sqlContext.sql(
            """
              |select a.day,a.mac,b.version
              |from login_table a
              |left join app_version_log b
              |on getApkVersion(a.version) = b.version
              |where b.version >= '3.1.4'
            """.stripMargin)
            .registerTempTable("white_login_table") //取出白猫版本的记录

          sqlContext.sql(
            """
              |select a.*,b.province
              |from white_login_table a join mac_area b
              |on a.mac = b.mac
            """.stripMargin)
          .registerTempTable("area_white_login_table") //关联取出有地区信息的白猫版本的记录


          val resultDf = sqlContext.sql(
            """
              |select province,count(distinct mac) as loginUser
              |from area_white_login_table
              |group by province
            """.stripMargin) //按照天、省统计mac数量

          val insertSql = s"insert into $table(day,province,loginUser) " +
            "values (?,?,?)"
          if (p.deleteOld) {
            val deleteSql = s"delete from $table where day=?"
            util.delete(deleteSql, insertDate)
          }

          resultDf.collect.foreach(e => {
            util.insert(insertSql, insertDate, e.get(0), e.get(1))
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
}
