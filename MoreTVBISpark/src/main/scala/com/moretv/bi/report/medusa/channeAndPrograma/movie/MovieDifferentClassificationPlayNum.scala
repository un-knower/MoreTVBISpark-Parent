package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.text.SimpleDateFormat
import java.util.Date

import com.moretv.bi.util.DBOperationUtils
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.util.parsing.json.JSONObject

/**
  * Created by Administrator on 2016/5/3.
  */
object MovieDifferentClassificationPlayNum extends BaseClass{
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
     /*该函数用于配置MovieSummaryViewNum*/
     val date = new Date()
     val dateFormat = new SimpleDateFormat("yyyyMMdd")
     val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

     /*设置参数*/
     val startDate = dateFormat.format(date)
     val numOfDays = "1"
     val logType = "play"
     val fileDir = "/log/medusa/parquet"
     val deleteOld = "true"
     val applicationMode = "medusa"
     val columns = "userId,event,contentType,pathMain"
     val dbMode = "medusa"
     val tableName = "medusa_programa_movie_classification_play_user_num"
    val sqlSpark = "select getPathMainInfo(pathMain,2,2),count(userId),count(distinct userId) from log_data " +
      "where event='startplay' and contentType='movie' and getPathMainInfo(pathMain,2,1)='movie' group by" +
      " " +
      "getPathMainInfo(pathMain,2,2)"
     val sqlInsert = "insert into medusa_programa_movie_classification_play_user_num(day,classification_name,view_num," +
       "user_num) values (?,?,?,?)"

     val jsonObj = new JSONObject(Map("startDate"->startDate,"numOfDays"->numOfDays,"logType"->logType,"fileDir"->fileDir,
                                       "deleteOld"->deleteOld,"applicationMode"->applicationMode,"columns"->columns,
                                       "tableName"->tableName,"dbMode"->dbMode,"sqlSpark"->sqlSpark,"sqlInsert"->sqlInsert))
     val insertConfigSql = "insert into statistic_app_config_info(app_name,logType,params_config,builder,build_time," +
       "job_application,template,remarks) values (?,?,?,?,?,?,?,?)"

     db.insert(insertConfigSql,"MovieDifferentClassificationPlayNum",logType,jsonObj.toString(),"夏俊","2016-05-05",
       "medusa","CountStatistic", "该应用用于统计频道与栏目编排中电影各个分类的详情页的浏览人数与次数！")
   }
 }
