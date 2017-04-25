package com.moretv.bi.report.medusa.entrance

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by baozhi.wang on 2017/3/20.
  * 脚本作用：统计不同入口播放统计 【频道及栏目编排-频道概况-「电影」-不同入口播放统计】
  * 基本逻辑：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
  *
  * 使用的维度表：
  *           专题维度表           dim_medusa_subject
  *           节目维度表           dim_medusa_program
  *           固定入口维度表        dim_medusa_fix_entrance_info
  * 使用的分析字段（从事实表获得）:
  *             userId           度量值
  *             duration         度量值
  *             videoSid         用来关联dim_medusa_program获得contentType
  *             subjectCode      事实表pathSpecial(3.x)和path(2.x)解析出的字段,在事实表ETL过程中会给出
  *             path             解析出入口区域launcher_area,入口位置launcherAccessLocation
  *             pathMain         解析出入口区域launcher_area,入口位置launcherAccessLocation
  *
  */
object ShortChannelEntrancePlayStatETL extends BaseClass {
  private val tableName = "contenttype_play_src_stat"
  private val fields = "day,contentType,entrance,pv,uv,duration"
  private val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?,?,?)"
  private val deleteSql = s"delete from $tableName where day = ? "
  private val spark_df_analyze_table = "analyze_table"
  private val isDebug = false

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        //引入维度表
        val dimension_subject_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        val dimension_program_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM)
        val dimensionProgramFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_program_input_dir)
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        println(s"--------------------dimensionProgramFlag is ${dimensionProgramFlag}")
        if (dimensionSubjectFlag && dimensionProgramFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM).registerTempTable(DimensionTypes.DIM_MEDUSA_PROGRAM)
        }else{
          throw new RuntimeException(s"--------------------dimension not exist")
        }

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var sqlStr = ""
        (0 until p.numOfDays).foreach(w => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.PLAY_VIEW_ETL, date).registerTempTable(spark_df_analyze_table)
          /*进入分析代码*/
          sqlStr = s"""
                      |select if(c.subject_content_type_name is not null,c.subject_content_type_name,b.content_type_name) as contentType,
                       |		  a.entryType,
                       |      count(a.userId)             as playNum,
                       |      count(distinct a.userId)    as playUser
                       | from
                       |	( select userId,entryType,duration,subjectCode,videoSid
                       |    from $spark_df_analyze_table
                       |    where event in ('$MEDUSA_EVENT_START_PLAY','$MORETV_EVENT_START_PLAY') ) a
                       | left join
                       |	${DimensionTypes.DIM_MEDUSA_PROGRAM} b
                       | on trim(a.videoSid)=trim(b.sid)
                       | left join
                       |	${DimensionTypes.DIM_MEDUSA_SUBJECT} c
                       | on a.subjectCode = c.subject_code
                       |group by if(c.subject_content_type_name is not null,c.subject_content_type_name,b.content_type_name),
                       |         a.entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val channel_entry_playNum_playUser_df = sqlContext.sql(sqlStr)
          channel_entry_playNum_playUser_df.registerTempTable("channel_entry_playNum_playUser_df")
          writeToHDFSForCheck(date,"channel_entry_playNum_playUser_df",channel_entry_playNum_playUser_df,p.deleteOld)
          sqlStr = s"""
                      |select if(c.subject_content_type_name is not null,c.subject_content_type_name,b.content_type_name) as contentType,
                      |		a.entryType,
                      |        sum(a.duration)             as duration_sum
                      | from
                      |	( select entryType,duration,subjectCode,videoSid
                      |   from $spark_df_analyze_table
                      |   where event not in ('$MEDUSA_EVENT_START_PLAY') and duration between 1 and 10800
                      |   ) a
                      | left join
                      |	${DimensionTypes.DIM_MEDUSA_PROGRAM} b
                      | on trim(a.videoSid)=trim(b.sid)
                      | left join
                      |	${DimensionTypes.DIM_MEDUSA_SUBJECT} c
                      | on a.subjectCode = c.subject_code
                      |group by if(c.subject_content_type_name is not null,c.subject_content_type_name,b.content_type_name),
                      |         a.entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val channel_entry_duration_df = sqlContext.sql(sqlStr)
          channel_entry_duration_df.registerTempTable("channel_entry_duration_df")
          writeToHDFSForCheck(date,"channel_entry_duration_df",channel_entry_duration_df,p.deleteOld)

          sqlStr = s"""
                      |select  a.contentType,
                      |        a.entryType,
                      |        a.playNum,
                      |        a.playUser,
                      |        round(b.duration_sum/a.playUser) as avg_duration
                      |from channel_entry_playNum_playUser_df    a
                      |     join
                      |     channel_entry_duration_df            b
                      |     on a.contentType=b.contentType and
                      |        a.entryType=b.entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val mysql_result_df = sqlContext.sql(sqlStr)
          writeToHDFSForCheck(date,"mysql_result_df",mysql_result_df,p.deleteOld)

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }
          //day,contentType,entrance,pv,uv,duration
          mysql_result_df.collect.foreach(row=>{
            var contentType=row.getString(0)
            if(null==contentType){
              contentType=row.getString(0)
            } else if(contentType.equalsIgnoreCase("电视剧")){
              contentType="电视"
            }else if(contentType.equalsIgnoreCase("记录片")){
              contentType="纪实"
            }
            util.insert(sqlInsert,sqlDate,contentType,row.getString(1),new JLong(row.getLong(2)),new JLong(row.getLong(3)),new JDouble(row.getDouble(4)))
          })
        })
      }
      case None => {
      }
    }
  }

  //用来写入HDFS，测试数据是否正确
  def writeToHDFSForCheck(date:String,logType:String,df:DataFrame,isDeleteOld:Boolean): Unit = {
    if (isDebug) {
      println(s"--------------------$logType is write done.")
      val outputPath = DataIO.getDataFrameOps.getPath(MERGER, logType, date)
      if (isDeleteOld) {
        HdfsUtil.deleteHDFSFile(outputPath)
      }
      df.write.parquet(outputPath)
    }
  }
}