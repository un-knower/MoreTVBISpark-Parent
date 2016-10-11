package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.text.SimpleDateFormat
import java.util.Date

import com.moretv.bi.util.DBOperationUtils
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.util.parsing.json.JSONObject

/**
  * Created by Administrator on 2016/5/3.
  */
object MovieSummaryPlayDurationNum extends BaseClass{
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(MovieSummaryPlayDurationNum,args)
  }
  override def execute(args: Array[String]) {
     /*该函数用于配置MovieSummaryViewNum*/
     val date = new Date()
     val dateFormat = new SimpleDateFormat("yyyyMMdd")
     val db = new DBOperationUtils("medusa")

     /*设置参数*/
     val startDate = dateFormat.format(date)
     val numOfDays = "1"
     val logType = "play"
     val fileDir = "/log/medusa/parquet"
     val deleteOld = "true"
     val applicationMode = "medusa"
     val columns = "userId,event,contentType,duration"
     val dbMode = "medusa"
     val tableName = "medusa_programa_movie_summary_play_duration"
     val sqlSpark = "select sum(duration),count(distinct userId) from log_data where event in ('userexit','selfend') and " +
       "contentType='movie'"
     val sqlInsert = "insert into medusa_programa_movie_summary_play_duration(day,play_duration) values (?,?)"

     val jsonObj = new JSONObject(Map("startDate"->startDate,"numOfDays"->numOfDays,"logType"->logType,"fileDir"->fileDir,
                                       "deleteOld"->deleteOld,"applicationMode"->applicationMode,"columns"->columns,
                                       "tableName"->tableName,"dbMode"->dbMode,"sqlSpark"->sqlSpark,"sqlInsert"->sqlInsert))
     val insertConfigSql = "insert into statistic_app_config_info(app_name,logType,params_config,builder,build_time," +
       "job_application,template,remarks) values (?,?,?,?,?,?,?,?)"

     db.insert(insertConfigSql,"MovieSummaryDurationNum",logType,jsonObj.toString(),"夏俊","2016-05-03","medusa",
       "CountStatistic",
       "该应用用于统计频道与栏目编排中详情页的播放人数！")
   }
 }
