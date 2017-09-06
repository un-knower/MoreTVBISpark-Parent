package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions.{col, udf}

/**
  * Created by zhu.bingxin on 2017/4/26.
  * 统计活跃用户数和登录用户数
  * 区分新老版本（3.1.4及以上的为新版本）
  */
object activeUserAndLoginUserCt extends BaseClass {


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
          val date2 = DateFormatUtils.readFormat.format(cal.getTime)

          //load data
          val logAccount = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date)
          val logLogin = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date2)
          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
            select("version").distinct().registerTempTable("app_version_log")

          //filter data
          logAccount.withColumn("day", col("openTime").substr(1, 10))
            .select("mac", "day","user_id", "current_version")
            .filter(s"day = '${insertDate}'")
            .registerTempTable("new_table")
          logLogin.select("day", "mac", "version","userId")
            .registerTempTable("login_table")

          //data processings
          sqlContext.sql(
            """
              |select x.version,count(distinct x.mac) as loginUser,count(distinct x.userId) as loginUser_userId
              |from
              |(select a.mac,a.userId,getVersion(b.version) as version
              |from login_table a
              |left join app_version_log b
              |on getApkVersion(a.version) = b.version) x
              |group by x.version
            """.stripMargin)
            .registerTempTable("login_users")
          sqlContext.sql(
            """
              |select x.version,count(distinct x.mac) as newUser,count(distinct x.user_id) as newUser_userId
              |from
              |(select a.mac,a.user_id,getVersion(b.version) as version
              |from new_table a
              |left join app_version_log b
              |on getApkVersion(a.current_version) = b.version) x
              |group by version
            """.stripMargin)
            .registerTempTable("new_users")
          val resultDf = sqlContext.sql(
            """
              |select a.version,a.loginUser,a.loginUser_userId,b.newUser,b.newUser_userId,a.loginUser-b.newUser as activeUser
              |from login_users a join new_users b
              |on a.version = b.version
            """.stripMargin)

          val insertSql = "insert into whiteMedusa_login_active_user_ct(day,version,loginUser,loginUser_userId,newUser,newUser_userId,activeUser) " +
            "values (?,?,?,?,?,?,?)"
          if (p.deleteOld) {
            val deleteSql = "delete from whiteMedusa_login_active_user_ct where day=?"
            util.delete(deleteSql, insertDate)
          }

          resultDf.collect.foreach(e => {
            util.insert(insertSql, insertDate, e.get(0), e.get(1), e.get(2), e.get(3), e.get(4), e.get(5))
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
