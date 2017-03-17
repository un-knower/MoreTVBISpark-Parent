package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by baozhi.wang on 2017/3/13.
  * 脚本作用：统计不同频道的专题播放量，用于展示在各个频道的专题播放趋势图以及内容评估的专题趋势图
  *
  * 逻辑方式【与EachChannelSubjectPlayInfo代码比较】：
  * 首先从日志里获取subject code，如果subject code为null,通过subject name关联维度表dim_medusa_subject获得subject code
  * 最终获得userId,subject code两个字段作为分析表
  * 最终目的：通过分析表和维度表dim_medusa_subject关联，获取分析结果
  *
  * 在下面注释的代码块内逻辑， 以后会迁移，用来做事实表ETL操作：
  * /*最终此逻辑会合并进入事实表的ETL过程-start*/
  * /*最终此逻辑会合并进入事实表的ETL过程-end*/
  * 事实表ETL目的：将日志一次性地完成ETL生成FACT_TABLE，之后其他分析脚本直接使用FACT_TABLE简单地获取想要分析的字段，
  *              例如：本脚本可以直接从FACT_TABLE获得userId,subjectCode进行分析，而不需在每个关于专题分析的脚本里，
  *                   重复的从原来的MERGER表获得subjectCode分析字段。
  */
object EachChannelSubjectPlayInfoExampleV2 extends BaseClass {
  private val mysql_analyze_result_table = "medusa_channel_subject_play_info_test"
  private val spark_df_analyze_table = "step3_table"

  private val playNumLimit = 5000
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        sqlContext.udf.register("getSubjectCode", PathParser.getSubjectCodeByPathETL _)
        sqlContext.udf.register("getSubjectName", PathParser.getSubjectNameByPathETL _)
        sqlContext.udf.register("getSubjectType", PathParser.getSubjectTypeByPathETL _)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        var sqlStr = ""
        val dimension_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        println(s"--------------------dimension_input_dir is $dimension_input_dir")
        val dimensionFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_input_dir)
        println(s"--------------------dimensionFlag is $dimensionFlag")
        if (dimensionFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
        }else{
          throw new RuntimeException(s"${DimensionTypes.DIM_MEDUSA_SUBJECT} not exist")
        }
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          /**最终此逻辑会合并进入事实表的ETL过程-start*/
          /**事实表数据处理步骤
            * 1.过滤单个用户播放单个视频量过大的情况
            * 2.为事实表生成完整的subject code
          */
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, date)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, date)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)
          if (medusaFlag && moretvFlag) {
            //step1
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, date).registerTempTable("medusa_table")
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.PLAYVIEW, date).registerTempTable("moretv_table")
            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       pathSpecial,
                       |       event,
                       |       'medusa'   as flag
                       |from medusa_table
                     """.stripMargin
            println("--------------------"+sqlStr)
            val medusa_table_rdd=sqlContext.sql(sqlStr).toJSON


            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       path,
                       |       event,
                       |       'moretv'   as flag
                       |from moretv_table
                     """.stripMargin
            val moretv_table_rdd=sqlContext.sql(sqlStr).toJSON
            val mergerRDD = medusa_table_rdd.union(moretv_table_rdd)
            var outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.STEP1,date)
            if(p.deleteOld){
              HdfsUtil.deleteHDFSFile(outputPath)
            }
            sqlContext.read.json(mergerRDD).registerTempTable("step1_table")
            sqlContext.read.json(mergerRDD).write.parquet(outputPath)

            //step2 filter
            //用于过滤单个用户播放当个视频量过大的情况
            sqlStr = s"""
                        |select concat(userId,videoSid) as filterColumn
                        |from step1_table
                        |group by concat(userId,videoSid)
                        |having count(1)>=$playNumLimit
                     """.stripMargin
            println("--------------------"+sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("step2_table_filter")

            sqlStr = s"""
                        |select a.userId,
                        |       a.videoSid,
                        |       a.pathSpecial,
                        |       a.path,
                        |       a.event,
                        |       a.flag
                        |from step1_table           a
                        |     left join
                        |     step2_table_filter    b
                        |     on concat(a.userId,a.videoSid)=b.filterColumn
                        |where b.filterColumn is null
                     """.stripMargin
            println("--------------------"+sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("step2_table")
            outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.STEP2,date)
            if(p.deleteOld){
              HdfsUtil.deleteHDFSFile(outputPath)
            }

            //step3 get subjet type is subject,not star,tag
            sqlStr = """
                 |select userId,
                 |       videoSid,
                 |       getSubjectName(pathSpecial)           as subjectName,
                 |       getSubjectCode(pathSpecial,'medusa')  as subjectCode
                 |from step2_table
                 |where flag='medusa'        and
                 |      event='startplay'    and
                 |      getSubjectType(pathSpecial,'medusa')='subject'
                 """.stripMargin
            println("--------------------"+sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("medusa_table_final")

            sqlStr = s"""
                       |select a.userId,
                       |       a.subjectName,
                       |       first(b.subject_code) as subjectCode
                       |from
                       |    (select userId,
                       |            subjectName
                       |     from medusa_table_final
                       |     where subjectCode is null
                       |     ) as a
                       |join
                       |    ${DimensionTypes.DIM_MEDUSA_SUBJECT} as b
                       |on a.subjectName=b.subject_name
                       |group by a.userId,
                       |         a.subjectName
                     """.stripMargin
            println("--------------------"+sqlStr)
            val medusa_subject_code_null_df=sqlContext.sql(sqlStr)

            sqlStr = """
                       |select userId,
                       |       subjectName,
                       |       subjectCode
                       |from  medusa_table_final
                       |where subjectCode is not null
                     """.stripMargin
            println("--------------------"+sqlStr)
            val medusa_subject_code_not_null_df=sqlContext.sql(sqlStr)
            val medusa_log_df = medusa_subject_code_null_df.unionAll(medusa_subject_code_not_null_df)

            sqlStr = """
                 |select userId,
                 |       ''                            as subjectName,
                 |       getSubjectCode(path,'moretv') as subjectCode
                 |from step2_table
                 |where event='playview' and
                 |      getSubjectCode(path,'moretv') is not null
                 """.stripMargin
            println("--------------------"+sqlStr)
            val moretv_log_df = sqlContext.sql(sqlStr)
            val step3_table_df=medusa_log_df.unionAll(moretv_log_df)
            step3_table_df.registerTempTable(spark_df_analyze_table)
            outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.STEP3,date)
            if(p.deleteOld){
              HdfsUtil.deleteHDFSFile(outputPath)
            }
            println("a--------------------medusa_log_df:"+medusa_log_df.schema.treeString+","+medusa_log_df.printSchema()+","+medusa_log_df.count())
            println("a--------------------moretv_log_df:"+moretv_log_df.schema.treeString+","+moretv_log_df.printSchema()+","+moretv_log_df.count())
            println("a--------------------step3_table_df:"+step3_table_df.schema.treeString+","+step3_table_df.printSchema()+","+step3_table_df.count())
          }else {
            throw new RuntimeException("2.x and 3.x log data is not exist")
          }
          /*最终此逻辑会合并进入事实表的ETL过程-end*/

          /*进入分析代码*/
          if (p.deleteOld) {
            val deleteSql = s"delete from $mysql_analyze_result_table where day=?"
            util.delete(deleteSql, insertDate)
          }
          sqlStr =
            s"""
               |select b.subject_content_type_name,
               |       count(userId) as play_num,
               |       count(distinct userId) as play_user
               |from $spark_df_analyze_table                  a join
               |     ${DimensionTypes.DIM_MEDUSA_SUBJECT}     b
               |     on a.subjectCode=b.subject_code
               |group by b.subject_content_type_name
           """.stripMargin
          println("analyse--------------------"+sqlStr)
          val sqlInsert = s"insert into $mysql_analyze_result_table(day,channel_name,play_num,play_user) values (?,?,?,?)"
          val analyse_resul_df=sqlContext.sql(sqlStr)
          println("c--------------------analyse_resul_df:"+analyse_resul_df.schema.treeString+","+analyse_resul_df.printSchema()+","+analyse_resul_df.count())

          analyse_resul_df.collect.foreach(row=>{
            util.insert(sqlInsert,insertDate,row.getString(0),new JLong(row.getLong(1)),new JLong(row.getLong(2)))
          })
        })
      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }
}