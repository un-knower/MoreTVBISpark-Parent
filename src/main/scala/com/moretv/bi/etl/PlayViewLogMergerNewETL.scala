package com.moretv.bi.etl

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by jiulin.wu on 2017/4/12.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/play
  * input: /mbi/parquet/playview/$date
  * output: /log/medusaAndMoretvMerger/$date/playview-etl
  */
object PlayViewLogMergerNewETL extends BaseClass {
  private val playNumLimit = 5000

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
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
        val dimension_page_entrance_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE)
        val dimensionPageEntranceFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_page_entrance_input_dir)
        val dimension_program_site_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP)
        val dimensionProgramSiteFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_program_site_input_dir)
        println(s"--------------------dimensionSourceSiteFlag is ${dimensionSourceSiteFlag}")
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        println(s"--------------------dimensionProgramFlag is ${dimensionProgramFlag}")
        println(s"--------------------dimensionPageEntranceFlag is ${dimensionPageEntranceFlag}")
        println(s"--------------------dimensionPageEntranceFlag is ${dimensionProgramSiteFlag}")
        if (dimensionSourceSiteFlag && dimensionSubjectFlag && dimensionProgramFlag && dimensionPageEntranceFlag && dimensionProgramSiteFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SOURCE_SITE).registerTempTable(DimensionTypes.DIM_MEDUSA_SOURCE_SITE)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM).registerTempTable(DimensionTypes.DIM_MEDUSA_PROGRAM)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE).registerTempTable(DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP).registerTempTable(DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP)
        } else {
          throw new RuntimeException(s"--------------------dimension not exist")
        }
        val moretv_table="moretv_table"
        val medusa_table="medusa_table"

        var sqlStr :String=""
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, inputDate)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, inputDate)
          val outputPath = DataIO.getDataFrameOps.getPath(MERGER, LogTypes.PLAY_VIEW_ETL, inputDate)
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
            medusaDf.registerTempTable(medusa_table)
            moretvDf.registerTempTable(moretv_table)

            //拆分出维度
            sqlStr = s"""
                       |select $medusaColNames,
                       |    getEntranceType(pathMain,'medusa')    as entryType,
                       |    getSubjectName(pathSpecial)           as subjectName,
                       |    getSubjectCode(pathSpecial,'medusa')  as subjectCode,
                       |    getListCategoryMedusa(pathMain,1)     as main_category,
                       |    getListCategoryMedusa(pathMain,2)     as second_category,
                       |    getListCategoryMedusa(pathMain,3)     as third_category
                       |from $medusa_table
                     """.stripMargin
            println("--------------------" + sqlStr)
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
            println("--------------------" + sqlStr)
            sqlContext.sql(sqlStr)
            val medusa_rdd = sqlContext.sql(sqlStr).toJSON

            val sqlSelectMoretv =s"""select $moretvColNames,
                                     |  getEntranceType(path,'moretv')     as entryType,
                                     |  getSubjectCode(path,'moretv')      as subjectCode,
                                     |  getListCategoryMoretv(path,1)      as main_category,
                                     |  getListCategoryMoretv(path,2)      as second_category,
                                     |  getListCategoryMoretv(path,3)      as third_category
                                     |from $moretv_table
                     """.stripMargin
            val moretv_rdd = sqlContext.sql(sqlSelectMoretv).toJSON
            //3.x and 2.x log merge
            val mergerRDD = medusa_rdd.union(moretv_rdd)
            val merge_table_df = sqlContext.read.json(mergerRDD).toDF()
            merge_table_df.cache()
            merge_table_df.registerTempTable("merge_table")
            val mergeColNames = merge_table_df.columns.toList.mkString(",")
            val mergeColNamesWithTable = merge_table_df.columns.toList.map(e=>{
              "a."+e
            }).mkString(",")
            val mergeColNamesWithTableWithout = merge_table_df.columns.toList.filter(e=>{
              e!="main_category" && e!="second_category" && e!="third_category"
            }).mkString(",")

            /** 用于过滤单个用户播放单个视频量过大的情况 */
            sqlStr =
              s"""
                 |select concat(userId,videoSid) as filterColumn
                 |from merge_table
                 |group by concat(userId,videoSid)
                 |having count(1)>=$playNumLimit
                     """.stripMargin
            println("--------------------" + sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("merge_table_filter")
            sqlStr =
              s"""
                 |select $mergeColNamesWithTable
                 |from merge_table              a
                 |     left join
                 |     merge_table_filter       b
                 |     on concat(a.userId,a.videoSid)=b.filterColumn
                 |where b.filterColumn is null
                     """.stripMargin
            println("--------------------" + sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("result_table")
            /**体育和少儿 关联查询获取中文名称*/
            //sport
            sqlStr =
              s"""
                |select $mergeColNamesWithTableWithout,
                |a.main_category,
                |b.area_name second_category,
                |c.third_category
                |from result_table a
                |left join
                |${DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE} b
                |on a.main_category=b.page_code and a.second_category=b.area_code
                |left join
                |${DimensionTypes.DIM_MEDUSA_SOURCE_SITE} c
                |on a.main_category=c.site_content_type and a.second_category<>'horizontal' and a.third_category=c.third_category_code
                |where a.main_category='sports'
              """.stripMargin
            println("--------------------" + sqlStr)
            val sportsRDD = sqlContext.sql(sqlStr)
            //kids
            sqlStr =
              s"""
                |select $mergeColNamesWithTableWithout,
                |a.main_category,
                |c.area_name second_category,
                |a.third_category
                |from result_table a
                |left join
                |${DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP} b
                |on a.second_category=b.path_code and a.second_category not in ('search','kids','comic','movie','kids_anim*搜一搜','my_tv','null') and a.second_category is not null
                |left join
                |${DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE} c
                |on b.program_code=c.area_code and c.area_name is not null and c.area_name<>'null'
                |where a.main_category='kids'
              """.stripMargin
            println("--------------------" + sqlStr)
            val kidsRDD = sqlContext.sql(sqlStr)
            /**合并数据之前先过滤原始数据集里的sports和kids的数据*/
            sqlStr =
              s"""
                |select $mergeColNames
                |from merge_table
                |where main_category<>'kids' and main_category<>'sports'
              """.stripMargin
            println("--------------------" + sqlStr)
            val otherRDD = sqlContext.sql(sqlStr)
            val resultRDD = sportsRDD.unionAll(kidsRDD).unionAll(otherRDD)
            val resultDF = resultRDD.toDF()
//            val result_df = sqlContext.sql(sqlStr)
            resultDF.write.parquet(outputPath)
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
