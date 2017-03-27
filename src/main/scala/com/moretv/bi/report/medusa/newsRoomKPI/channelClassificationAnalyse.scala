package com.moretv.bi.report.medusa.newsRoomKPI

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.parse.ReadConfig
import com.moretv.bi.etl.MvDimensionClassificationETL
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.{UDFConstantDimension, PathParser}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by baozhi.wang on 2017/3/20.
  * 脚本作用： 频道分类统计的播放次数播放人数统计【频道及栏目编排-频道分类统计-「电影,电视剧」-频道分类统计】
  *
  * 参考资料：收集分布在资源调度平台的sql语句【http://172.16.17.100:8090/pages/viewpage.action?pageId=4424217】
  *         使用事实表解析出来的各个频道的一级分类，二级分类，三级分类，四级分类
  *         数仓中以表的形式来分析维度：http://172.16.17.100:8090/pages/viewpage.action?pageId=4425569
  *         play事实表维度抽取表达式: http://172.16.17.100:8090/pages/viewpage.action?pageId=4424612
  *
  * 原有统计逻辑
  * 【各个频道的分析sql是分布在资源调度平台上，需整理到一起，分析共性。】
  *
  * 需要做：
  *  1.列表页维度 main_category,second_category,third_category,fourth_category,fifth_category,list_index
  *  2.推荐入口维度 recommend_source_type,recommend_property,pre_content_type,recommend_method
  *


  * 使用的事实表：
  *           解析出列表页维度  一级入口，二级入口
  * 使用的维度表：
  *           节目维度表           dim_medusa_source_site [关联此表做过滤]

  * 使用的分析字段（从事实表获得）:
  *             userId           度量值
  *             duration         度量值
  *             videoSid         用来关联dim_medusa_program获得contentType
  *             subjectCode      事实表pathSpecial(3.x)和path(2.x)解析出的字段,在事实表ETL过程中会给出
  *             path             解析出入口区域launcher_area,入口位置launcherAccessLocation
  *             pathMain         解析出入口区域launcher_area,入口位置launcherAccessLocation
  *
  */
object channelClassificationAnalyse extends BaseClass {
  private val tableName = "电影表"
  private val fields = "day,contentType,entrance,pv,uv,duration"
  private val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?,?,?)"
  private val deleteSql = s"delete from $tableName where day = ? "
  private val playNumLimit = 5000
  private val spark_df_analyze_table = "channel_classification_analyze_table"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        println("---------------------"+ReadConfig.getConfig)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        sqlContext.udf.register("getSubjectCode", PathParser.getSubjectCodeByPathETL _)
        sqlContext.udf.register("getSubjectName", PathParser.getSubjectNameByPathETL _)
        sqlContext.udf.register("getSubjectType", PathParser.getSubjectTypeByPathETL _)
        sqlContext.udf.register("getListCategoryMedusa", PathParser.getListCategoryMedusaETL _)
        sqlContext.udf.register("getListCategoryMoretv", PathParser.getListCategoryMoretvETL _)

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

            /**
              *解析出列表页维度  一级入口，二级入口 */
            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       pathMain,
                       |       event,
                       |       getListCategoryMedusa(pathMain,1) as main_category,
                       |       getListCategoryMedusa(pathMain,2) as sub_category,
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
                       |       getListCategoryMoretv(path,1)  as main_category,
                       |       getListCategoryMoretv(path,2)  as sub_category,
                       |       'moretv'   as flag
                       |from moretv_table
                     """.stripMargin
            val moretv_table_rdd=sqlContext.sql(sqlStr).toJSON
            val mergerRDD = medusa_table_rdd.union(moretv_table_rdd)
            val step1_table_df = sqlContext.read.json(mergerRDD)
            step1_table_df.registerTempTable("step1_table")
            writeToHDFSForCheck(date,"step1_table_df",step1_table_df,p.deleteOld)

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
                        |       a.pathMain,
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
            val step2_table_df=sqlContext.sql(sqlStr)
            step2_table_df.cache()
            step2_table_df.registerTempTable(spark_df_analyze_table)
            writeToHDFSForCheck(date,spark_df_analyze_table,step2_table_df,p.deleteOld)
        }else {
            throw new RuntimeException("2.x and 3.x log data is not exist")
          }
          /*最终此逻辑会合并进入事实表的ETL过程-end*/

          /*进入分析代码*/
          sqlStr = s"""
                     |select if(subject_content_type_name is not null,subject_content_type_name,content_type_name) as contentType,
                     |        entryType,
                     |        count(userId)             as playNum,
                     |        count(distinct userId)    as playUser
                     |from $spark_df_analyze_table
                     |where event in ('$MEDUSA_EVENT_START_PLAY','$MORETV_EVENT_START_PLAY')
                     |group by if(subject_content_type_name is not null,subject_content_type_name,content_type_name),
                     |         entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val channel_entry_playNum_playUser_df = sqlContext.sql(sqlStr)
          channel_entry_playNum_playUser_df.registerTempTable("channel_entry_playNum_playUser_df")
          writeToHDFSForCheck(date,"channel_entry_playNum_playUser_df",channel_entry_playNum_playUser_df,p.deleteOld)


          sqlStr = s"""
                      |select if(subject_content_type_name is not null,subject_content_type_name,content_type_name) as contentType,
                      |        entryType,
                      |        sum(duration)             as duration_sum
                      |from $spark_df_analyze_table
                      |where event not in ('$MEDUSA_EVENT_START_PLAY') and
                      |      duration between 1 and 10800
                      |group by if(subject_content_type_name is not null,subject_content_type_name,content_type_name),
                      |         entryType
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
            util.insert(sqlInsert,sqlDate,row.getString(0),row.getString(1),new JLong(row.getLong(2)),new JLong(row.getLong(3)),new JDouble(row.getDouble(4)))
          })
        })
      }
      case None => {
      }
    }
  }

  //用来写入HDFS，测试数据是否正确
  def writeToHDFSForCheck(date:String,logType:String,df:DataFrame,isDeleteOld:Boolean): Unit ={
    println(s"--------------------$logType is write done.")
    val outputPath= DataIO.getDataFrameOps.getPath(MERGER,logType,date)
    if(isDeleteOld){
      HdfsUtil.deleteHDFSFile(outputPath)
    }
    df.write.parquet(outputPath)
  }





}