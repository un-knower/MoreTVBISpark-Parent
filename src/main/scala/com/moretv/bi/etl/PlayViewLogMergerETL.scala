package com.moretv.bi.etl

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DimensionTypes, LogTypes}
import com.moretv.bi.logETL.KidsPathParser
import com.moretv.bi.report.medusa.entrance.ChannelEntrancePlayStatETL._
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by baozhi.wang on 2017/3/14.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/play
  * input: /mbi/parquet/playview/$date
  * output: /log/medusaAndMoretv/parquet/$date/playviewETL
  *
  * play事实表ETL需要做的任务：
  * 1.解析subject code维度【参考com.moretv.bi.report.medusa.subject.EachChannelSubjectPlayInfoETL代码】
  * 2.解析列表页维度【
  *       普通类型：参考com.moretv.bi.report.medusa.channelClassification.ChannelClassificationStatETL
  *       少儿类型：参考com.moretv.bi.logETL.KidsPathParser
  *       音乐类型：参考佳莹代码
  *       体育类型：参考com.moretv.bi.logETL.SportsPathParser】
  * 3.解析首页入口维度
  *   参考：com.moretv.bi.report.medusa.entrance.ChannelEntrancePlayStatETL
  *
  * 过滤播放量大于5000的记录【参考com.moretv.bi.report.medusa.subject.EachChannelSubjectPlayInfoETL代码】
  * 移动到其他类做，保留最原始数据，方便更改规则后批量刷数据
  *
  */
object PlayViewLogMergerETL extends BaseClass {
  private val playNumLimit = 5000

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        sqlContext.udf.register("pathParser", PathParserETL.pathParser _)
        sqlContext.udf.register("getSubjectCode", PathParserETL.getSubjectCodeByPathETL _)
        sqlContext.udf.register("getSubjectName", PathParserETL.getSubjectNameByPathETL _)
        sqlContext.udf.register("getEntranceType", PathParserETL.getEntranceTypeByPathETL _)
        sqlContext.udf.register("getListCategoryMedusa", PathParserETL.getListCategoryMedusaETL _)
        sqlContext.udf.register("getListCategoryMoretv", PathParserETL.getListCategoryMoretvETL _)

        //引入维度表
        val dimension_source_site_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SOURCE_SITE)
        val dimensionSourceSiteFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_source_site_input_dir)
        val dimension_subject_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        val dimension_program_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM)
        val dimensionProgramFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_program_input_dir)
        println(s"--------------------dimensionSourceSiteFlag is ${dimensionSourceSiteFlag}")
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        println(s"--------------------dimensionProgramFlag is ${dimensionProgramFlag}")
        if (dimensionSourceSiteFlag && dimensionSubjectFlag && dimensionProgramFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SOURCE_SITE).registerTempTable(DimensionTypes.DIM_MEDUSA_SOURCE_SITE)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM).registerTempTable(DimensionTypes.DIM_MEDUSA_PROGRAM)
        } else {
          throw new RuntimeException(s"--------------------dimension not exist")
        }
        val moretv_table="moretv_table"
        val medusa_table="medusa_table"
        val moretv_table_filtered="moretv_table_filtered"
        val medusa_table_filtered="medusa_table_filtered"

        var sqlStr :String=""
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, inputDate)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, inputDate)
          val outputPath = DataIO.getDataFrameOps.getPath(MERGER, LogTypes.PLAY_VIEW_2_FILTER_ETL, inputDate)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)

          if (p.deleteOld) {
            HdfsUtil.deleteHDFSFile(outputPath)
          }
          if (medusaFlag && moretvFlag) {
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, inputDate)
            val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.PLAYVIEW, inputDate)
            val medusaColumnList=medusaDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            })
            val medusaColNames = medusaColumnList.mkString(",")
            val medusaColNamesWithTable = medusaColumnList.map(e=>{
              "a."+e
            }).mkString(",")
            val moretvColumnList = moretvDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            })
            val moretvColNames = moretvColumnList.mkString(",")
            val moretvColNamesWithTable = moretvColumnList.map(e=>{
              "a."+e
            }).mkString(",")
            medusaDf.registerTempTable(medusa_table)
            moretvDf.registerTempTable(moretv_table)

            /** 3.x用于过滤单个用户播放单个视频量过大的情况 */
            sqlStr =
              s"""
                 |select concat(userId,videoSid) as filterColumn
                 |from $medusa_table
                 |group by concat(userId,videoSid)
                 |having count(1)>=$playNumLimit
                     """.stripMargin
            println("--------------------" + sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("medusa_table_filter")
            sqlStr =
              s"""
                 |select $medusaColNamesWithTable
                 |from $medusa_table             a
                 |     left join
                 |     medusa_table_filter       b
                 |     on concat(a.userId,a.videoSid)=b.filterColumn
                 |where b.filterColumn is null
                     """.stripMargin
            println("--------------------" + sqlStr)
            val medusa_table_after_filter_df = sqlContext.sql(sqlStr)
            medusa_table_after_filter_df.cache()
            medusa_table_after_filter_df.registerTempTable(medusa_table_filtered)

            /** 2.x用于过滤单个用户播放单个视频量过大的情况 */
            sqlStr =
              s"""
                 |select concat(userId,videoSid) as filterColumn
                 |from $moretv_table
                 |group by concat(userId,videoSid)
                 |having count(1)>=$playNumLimit
                     """.stripMargin
            println("--------------------" + sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("moretv_table_filter")
            sqlStr =
              s"""
                 |select $moretvColNamesWithTable
                 |from $moretv_table             a
                 |     left join
                 |     moretv_table_filter       b
                 |     on concat(a.userId,a.videoSid)=b.filterColumn
                 |where b.filterColumn is null
                     """.stripMargin
            println("--------------------" + sqlStr)
            val moretv_table_after_filter_df = sqlContext.sql(sqlStr)
            moretv_table_after_filter_df.cache()
            moretv_table_after_filter_df.registerTempTable(moretv_table_filtered)

            //拆分出维度
            sqlStr = s"""
                       |select $medusaColNames,
                       |    getEntranceType(pathMain,'medusa')    as entryType,
                       |    getSubjectName(pathSpecial)           as subjectName,
                       |    getSubjectCode(pathSpecial,'medusa')  as subjectCode,
                       |    getListCategoryMedusa(pathMain,1)     as main_category,
                       |    getListCategoryMedusa(pathMain,2)     as second_category,
                       |    getListCategoryMedusa(pathMain,3)     as third_category
                       |from $medusa_table_filtered
                     """.stripMargin
            val medusa_table_init_table_df=sqlContext.sql(sqlStr)
            medusa_table_init_table_df.registerTempTable("medusa_table_init_table")
            //解析subject code维度,解析不到，使用subject维度表补全
            sqlStr = s"""
                        |select  $medusaColNamesWithTable,
                        |        a.entryType,
                        |        a.main_category,
                        |        a.second_category,
                        |        a.third_category,
                        |       if((a.subjectCode is null or a.subjectCode=''),b.subject_code,a.subjectCode) as subjectCode
                        |from medusa_table_init_table a
                        |left join
                        |    (select subject_name,
                        |            first(subject_code) as subject_code
                        |     from
                        |     ${DimensionTypes.DIM_MEDUSA_SUBJECT}
                        |     group by subject_name
                        |    ) b
                        |on trim(a.subjectName)=trim(b.subject_name)
                     """.stripMargin
            sqlContext.sql(sqlStr)
            val medusa_rdd = sqlContext.sql(sqlStr).toJSON

            val sqlSelectMoretv =s"""select $moretvColNames,
                                     |  getEntranceType(pathMain,'moretv') as entryType,
                                     |  getSubjectCode(path,'moretv')      as subjectCode,
                                     |  getListCategoryMoretv(path,1)      as main_category,
                                     |  getListCategoryMoretv(path,2)      as second_category,
                                     |  getListCategoryMedusa(path,3)      as third_category
                                     |from $moretv_table
                     """.stripMargin
            val moretv_rdd = sqlContext.sql(sqlSelectMoretv).toJSON
            //3.x and 2.x log merge
            val mergerDf = medusa_rdd.union(moretv_rdd)
            sqlContext.read.json(mergerDf).write.parquet(outputPath)
          } else if (!medusaFlag && moretvFlag) {
            throw new RuntimeException("medusaFlag is false")
          } else if (medusaFlag && !moretvFlag) {
            throw new RuntimeException("moretvFlag is false")
          }
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
