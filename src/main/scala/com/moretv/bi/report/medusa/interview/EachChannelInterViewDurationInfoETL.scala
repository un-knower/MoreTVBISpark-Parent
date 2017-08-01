package com.moretv.bi.report.medusa.interview

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.SQLContext
import org.json4s.JsonAST.JDouble

/**
  * 创建人：吴久林
  * 创建时间：2017/4/7
  * 程序作用：分析各个频道的浏览时长
  * 数据输入：
  * 数据输出：
  */
object EachChannelInterViewDurationInfoETL extends BaseClass  {
  private val fields_mv = "day,channel,view_duration,user"
  private val fields_avg = "day,channel,view_duration,avg_duration"
  private val fields_other = "day,channel,view_duration"
  private val channel_to_mysql_table=Map(
    CHANNEL_KIDS->"medusa_channel_view_duration_kids_info",
    CHANNEL_OPERA->"medusa_channel_view_duration_xiqu_info",
    CHANNEL_COMIC->"medusa_channel_view_duration_comic_info",
    CHANNEL_VARIETY_PROGRAM->"medusa_channel_view_duration_zongyi_info",
    CHANNEL_HOT->"medusa_channel_view_duration_hot_info",
    CHANNEL_MV->"medusa_channel_view_duration_mv_info",
    CHANNEL_RECORD->"medusa_channel_view_duration_jilu_info",
    CHANNEL_TV->"medusa_channel_view_duration_tv_info",
    CHANNEL_MOVIE->"medusa_channel_view_duration_movie_info"
  )
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }
  override def execute(args: Array[String]): Unit ={
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(w => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.INTERVIEW_ETL, date).registerTempTable("interview_etl")

          //删除历史记录
          val channelArray = Array(CHANNEL_KIDS,CHANNEL_OPERA,CHANNEL_COMIC,CHANNEL_VARIETY_PROGRAM,CHANNEL_HOT,CHANNEL_MV,CHANNEL_RECORD,CHANNEL_TV,CHANNEL_MOVIE)
          if (p.deleteOld) {
            for(channel_name <-channelArray){
              val tableName = channel_to_mysql_table.get(channel_name).get
              val deleteSql = s"delete from $tableName where day = ? "
              util.delete(deleteSql, sqlDate)
            }
          }
          //业务分析
          for(channel_name <-channelArray){
            val tableName = channel_to_mysql_table.get(channel_name).get
            var sqlStr = ""
            var sqlSpark = ""
            if (channel_name == "mv"){
              sqlStr = s"insert into $tableName($fields_mv) values(?,?,?,?)"
              sqlSpark =
                s"""
                   | select cast(round(sum(duration)) as long) as view_duration,
                   | count(distinct userId) as user
                   | from interview_etl
                   | where contentType ='$channel_name' and event='exit'
                   | and duration>=0 and duration<=36000
             """.stripMargin
            }

            if (channel_name=="kids" || channel_name=="tv" || channel_name=="movie"){
              sqlStr = s"insert into $tableName($fields_avg) values(?,?,?,?)"
              var whereSql = " and duration>=0 and duration<=36000"
              if (channel_name == "tv") {
                whereSql = " and duration>=0 and duration<=10800"
              }
              sqlSpark =
                s"""
                   | select cast(round(sum(duration)) as long) as view_duration,
                   | sum(duration)/count(distinct userId) as avg_duration
                   | from interview_etl
                   | where contentType ='$channel_name' and event='exit' $whereSql
             """.stripMargin
            }

            if (channel_name=="jilu" || channel_name=="comic" || channel_name=="xiqu" || channel_name=="zongyi" || channel_name=="hot"){
              sqlStr = s"insert into $tableName($fields_other) values(?,?,?)"
              sqlSpark =
                s"""
                   | select cast(round(sum(duration)) as long) as view_duration
                   | from interview_etl
                   | where contentType ='$channel_name' and event='exit'
                   | and duration>=0 and duration<=10800
             """.stripMargin
            }


           sqlContext.sql(sqlSpark).collect.foreach(row=>{
              if (channel_name=="jilu" || channel_name=="comic" || channel_name=="xiqu" || channel_name=="zongyi" || channel_name=="hot"){
                val view_duration = row.getLong(0)
                util.insert(sqlStr,sqlDate,channel_name,view_duration)
              } else if (channel_name == "mv"){
                val view_duration = row.getLong(0)
                val user = row.getLong(1)
                util.insert(sqlStr,sqlDate,channel_name,view_duration,user)
              } else {
                val view_duration = row.getLong(0)
                val avg_duration = row.getDouble(1)
                util.insert(sqlStr,sqlDate,channel_name,view_duration,avg_duration)
              }
            })

          }

        })
      }
      case None => {
      }
    }
  }

}
