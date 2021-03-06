package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, ParamsParseUtil}

/**
  * Created by Chubby on 2017/5/8.
  * 该类用于统计升级过白猫版本的用户信息（UserId层面）
  */
object WhiteMedusaUpdatedUserByUid extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        /**
          * 加载app版本号的维度表
          */
        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i=>{
          val loadDate = DateFormatUtils.readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH,1)
          val outDir = s"/log/medusa/parquet/${loadDate}/white_medusa_update_user_by_uid"


          /**
            * 加载登录信息
            */
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, loadDate).
            registerTempTable("today_login_log")
          sqlContext.sql(
            """
              |select a.mac,b.version,a.date,a.userId
              |from today_login_log as a
              |left join app_version_log as b
              |on getApkVersion(a.version) = b.version
              |where a.mac is not null and b.version>='3.1.4'
            """.stripMargin).registerTempTable("white_medusa_login_log")

          if(p.deleteOld) HdfsUtil.deleteHDFSFile(outDir)

          if(loadDate.equals("20170426")){
            sqlContext.sql(
              """
                |select distinct mac,date,userId
                |from white_medusa_login_log
              """.stripMargin).write.parquet(outDir)
          }else{
            val previousCal = Calendar.getInstance()
            previousCal.setTime(DateFormatUtils.readFormat.parse(loadDate))
            previousCal.add(Calendar.DAY_OF_MONTH, -1)
            val previousDate = DateFormatUtils.readFormat.format(previousCal.getTime)
            DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.WHITE_MEDUSA_UPDATE_USER_BY_UID,previousDate).
              registerTempTable("previous_white_medusa_login_log")
            sqlContext.sql(
              """
                |select a.userId,b.userId
                |from white_medusa_login_log as a
                |left join previous_white_medusa_login_log as b
                |on a.userId = b.userId
              """.stripMargin).toDF("uid_a","uid_b").filter("uid_b is null").registerTempTable("new_update_uid")
            sqlContext.sql(
              """
                |select mac,date,userId
                |from previous_white_medusa_login_log
                |union
                |select distinct a.mac,date,userId
                |from white_medusa_login_log as a
                |join new_update_uid as b
                |on a.userId = b.uid_a
              """.stripMargin).repartition(200).write.parquet(outDir)
          }
        })

      }
      case None => {}
    }
  }
}
