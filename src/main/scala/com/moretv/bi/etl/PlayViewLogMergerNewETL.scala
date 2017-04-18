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
        val dimension_subject_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        val dimension_program_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM)
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

        var sqlStr :String = ""
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
            val moretvColNamesWithTable = moretvColumnList.map(e=>{
              "a."+e
            }).mkString(",")
            medusaDf.registerTempTable(medusa_table)
            moretvDf.registerTempTable(moretv_table)

            //medusa 拆分出维度
            sqlStr = s"""
                       |select $medusaColNames,
                       |    getEntranceType(pathMain,'medusa')    as entryType,
                       |    getSubjectName(pathSpecial)           as subjectName,
                       |    getSubjectCode(pathSpecial,'medusa')  as subjectCode,
                       |    getListCategoryMedusa(pathMain,1)     as main_category,
                       |    getListCategoryMedusa(pathMain,2)     as second_category,
                       |    getListCategoryMedusa(pathMain,3)     as third_category,
                       |    'medusa' flag
                       |from $medusa_table
                     """.stripMargin
            println("medusa 1--------------------" + sqlStr)
            val medusa_table_init_table_df = sqlContext.sql(sqlStr)
            println("medusa count1 :" + medusa_table_init_table_df.count())
            medusa_table_init_table_df.registerTempTable("medusa_table_init_table")
            /**解析subject code维度,解析不到，使用subject维度表补全 */
            sqlStr = s"""
                        |select  $medusaColNamesWithTable,
                        |        a.entryType,
                        |        a.main_category,
                        |        a.second_category,
                        |        a.third_category,
                        |        a.flag,
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
            println("medusa 2--------------------" + sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("medusa_filter")
//            println("medusa count2: "+sqlContext.sql(sqlStr).count())
            /** 3.x 使用站点树维度表对3.x的二级入口进行过滤，防止日志里脏数据(除了体育 少儿 一级入口) */
              /** step1:先处理除了sports和kids的数据 (因为最后关联查询入口的中文名称这一步只是针对sports和kids)*/
            sqlStr =
              s"""
                |select $medusaColNamesWithTable,
                |a.entryType,
                |a.main_category,
                |if(a.second_category is null or a.second_category='','',b.second_category) second_category,
                |a.third_category third_category,
                |a.flag,
                |a.subjectCode
                |from medusa_filter a
                |left join
                |   (
                |    select site_content_type,
                |    max(second_category) second_category,
                |    max(main_category_code) main_category_code from
                |    ${DimensionTypes.DIM_MEDUSA_SOURCE_SITE}
                |    where site_content_type is not null
                |    group by site_content_type
                |   ) b
                |on a.main_category=b.site_content_type and a.second_category=b.second_category and b.main_category_code!='program_site'
                |where a.main_category not in ('$CHANNEL_SPORTS','$CHANNEL_KIDS') or a.main_category is null
              """.stripMargin
            println("medusa 3--------------------" + sqlStr)
            val medusa_step1_df = sqlContext.sql(sqlStr)
//            println("medusa count3: "+medusa_step1_df.count())
              /** step2:单独拉出sports和kids的数据*/
            sqlStr =
              s"""
                |select $medusaColNamesWithTable,
                |a.entryType,
                |a.main_category,
                |a.second_category second_category,
                |a.third_category third_category,
                |a.flag,
                |a.subjectCode
                |from medusa_filter a
                |where a.main_category in ('$CHANNEL_SPORTS','$CHANNEL_KIDS')
              """.stripMargin
            println("medusa 4--------------------" + sqlStr)
            val medusa_step2_df = sqlContext.sql(sqlStr)
//            println("medusa count4: "+medusa_step2_df.count())
              /** 合并以上两步的数据*/
            val medusa_rdd = medusa_step1_df.unionAll(medusa_step2_df).toJSON
//            println("medusa count5: "+medusa_rdd.count())

            //moretv 日志处理
            var sqlSelectMoretv =s"""select $moretvColNames,
                                     |  getEntranceType(path,'moretv')     as entryType,
                                     |  getSubjectCode(path,'moretv')      as subjectCode,
                                     |  getListCategoryMoretv(path,1)      as main_category,
                                     |  getListCategoryMoretv(path,2)      as second_category,
                                     |  getListCategoryMoretv(path,3)      as third_category,
                                     |  'moretv' flag
                                     |from $moretv_table
                     """.stripMargin
            println("moretv 1--------------------" + sqlSelectMoretv)
            sqlContext.sql(sqlSelectMoretv).registerTempTable("moretv_filter")
            println("moretv count1: "+sqlContext.sql(sqlSelectMoretv).count())
            /** 2.x 使用站点树维度表对2.x的二级入口进行过滤，英文转中文 (除了体育 少儿 一级入口) */
              /** step1:先处理除了sports和kids的数据 (因为最后关联查询入口的中文名称这一步只是针对sports和kids)*/
            sqlSelectMoretv =
              s"""
                |select $moretvColNamesWithTable,
                |a.entryType,
                |a.subjectCode,
                |a.main_category,
                |if(a.second_category is null or a.second_category='','',b.second_category) second_category,
                |a.third_category third_category,
                |a.flag
                |from moretv_filter a
                |left join
                |   (
                |    select site_content_type,
                |    max(second_category) second_category,
                |    max(second_category_code) second_category_code,
                |    max(main_category_code) main_category_code from
                |    ${DimensionTypes.DIM_MEDUSA_SOURCE_SITE}
                |    where site_content_type is not null
                |    group by site_content_type
                |   ) b
                |on a.main_category=b.site_content_type and a.second_category=b.second_category_code and b.main_category_code!='program_site'
                |where (a.main_category not in ('$CHANNEL_SPORTS','$CHANNEL_KIDS') or a.main_category is null)
              """.stripMargin
            println("moretv 2--------------------" + sqlSelectMoretv)
            val moretv_step1_df = sqlContext.sql(sqlSelectMoretv)
//            println("moretv count2: "+moretv_step1_df.count())
              /** step2:单独拉出sports和kids的数据*/
            sqlSelectMoretv =
              s"""
                |select $moretvColNamesWithTable,
                |a.entryType,
                |a.subjectCode,
                |a.main_category,
                |a.second_category second_category,
                |a.third_category third_category,
                |a.flag
                |from moretv_filter a
                |where a.main_category in ('$CHANNEL_SPORTS','$CHANNEL_KIDS')
              """.stripMargin
            println("moretv 3--------------------" + sqlSelectMoretv)
            val moretv_step2_df = sqlContext.sql(sqlSelectMoretv)
//            println("moretv count3: "+moretv_step2_df.count())
              /** 合并以上两步的数据*/
            val moretv_rdd = moretv_step1_df.unionAll(moretv_step2_df).toJSON
//            println("moretv count4: "+moretv_rdd.count())

            //3.x and 2.x log merge
            val mergerRDD = medusa_rdd.union(moretv_rdd)
            val merge_table_df = sqlContext.read.json(mergerRDD).toDF()
            println("merge count: "+merge_table_df.count())
            merge_table_df.cache()
            merge_table_df.registerTempTable("merge_table")
            val mergeColumnList = merge_table_df.columns.toList
            val mergeColNamesWithTable = mergeColumnList.map(e=>{
              "a."+e
            }).mkString(",")
            val mergeColNamesWithTableWithout = mergeColumnList.filter(e=>{
              e!="main_category" && e!="second_category" && e!="third_category"
            }).map(e=>{
              "a."+e
            }).mkString(",")
            val mergeColNamesWithout = mergeColumnList.filter(e=>{
              e!="main_category" && e!="second_category" && e!="third_category"
            }).mkString(",")

            /**体育和少儿 关联查询获取中文名称*/
            //sport
            sqlStr =
              s"""
                |select $mergeColNamesWithTableWithout,
                |a.main_category,
                |b.area_name second_category,
                |c.third_category
                |from merge_table a
                |left join
                |   (
                |    select page_code,max(area_code) area_code,max(area_name) area_name from
                |    ${DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE} group by page_code
                |   ) b
                |on a.main_category=b.page_code and a.second_category=b.area_code
                |left join
                |   (
                |    select site_content_type,max(third_category_code) third_category_code,max(third_category) third_category from
                |    ${DimensionTypes.DIM_MEDUSA_SOURCE_SITE} group by site_content_type
                |   ) c
                |on a.main_category=c.site_content_type and a.second_category<>'horizontal' and a.third_category=c.third_category_code
                |where a.main_category='$CHANNEL_SPORTS'
              """.stripMargin
            println("sports----------"+sqlStr)
            val sportsDF = sqlContext.sql(sqlStr)
//            println("sports count: "+sportsDF.count())
            //kids
            sqlStr =
              s"""
                |select $mergeColNamesWithTableWithout,
                |a.main_category,
                |if(c.area_name='' or c.area_name='null','',c.area_name) second_category,
                |a.third_category
                |from merge_table a
                |left join
                |   (
                |    select distinct path_code,program_code from
                |    ${DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP}
                |   ) b
                |on a.second_category=b.path_code and a.second_category is not null
                |left join
                |   (
                |    select area_code,max(area_name) area_name from
                |    ${DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE} group by area_code
                |   ) c
                |on b.program_code=c.area_code and c.area_name is not null
                |where a.main_category='$CHANNEL_KIDS'
              """.stripMargin
            println("kids----------"+sqlStr)
            val kidsDF = sqlContext.sql(sqlStr)
//            println("kids count: "+kidsDF.count())
            /**合并数据之前先过滤原始数据集里的sports和kids的数据*/
            sqlStr =
              s"""
                |select $mergeColNamesWithout,
                |main_category,
                |second_category,
                |third_category
                |from merge_table
                |where main_category not in ('$CHANNEL_KIDS','$CHANNEL_SPORTS') or main_category is null
              """.stripMargin
            println("other----------"+sqlStr)
            val otherDF = sqlContext.sql(sqlStr)
//            println("other count: "+otherDF.count())
            val allDF = sportsDF.unionAll(kidsDF).unionAll(otherDF)
            allDF.registerTempTable("result_table")
            println("all count: "+allDF.count())

            /** 用于过滤单个用户播放单个视频量过大的情况 */
            sqlStr =
              s"""
                 |select concat(userId,videoSid) as filterColumn
                 |from result_table
                 |group by concat(userId,videoSid)
                 |having count(1)>=$playNumLimit
                     """.stripMargin
            println("filter step1: "+sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("table_filter")
            sqlStr =
              s"""
                 |select $mergeColNamesWithTable
                 |from result_table              a
                 |     left join
                 |     table_filter       b
                 |     on concat(a.userId,a.videoSid)=b.filterColumn
                 |where b.filterColumn is null
                     """.stripMargin
            println("filter step2: "+sqlStr)
            val resultDF = sqlContext.sql(sqlStr)
            println("result count: "+resultDF.count())
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
