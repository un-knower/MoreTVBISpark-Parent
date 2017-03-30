package com.moretv.bi.report.medusa.channelClassification

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.parse.ReadConfig
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by baozhi.wang on 2017/3/27.
  * 脚本作用： 1.作为解析列表页维度的示范例子
  * 2.频道分类统计的播放次数播放人数统计【频道及栏目编排-频道分类统计-「电影,电视剧」-频道分类统计】
  *
  * 参考资料：收集分布在资源调度平台的sql语句【http://172.16.17.100:8090/pages/viewpage.action?pageId=4424217】
  * 使用事实表解析出来的各个频道的一级分类，二级分类，三级分类，四级分类
  * 数仓中以表的形式来分析维度：http://172.16.17.100:8090/pages/viewpage.action?pageId=4425569
  * play事实表维度抽取表达式: http://172.16.17.100:8090/pages/viewpage.action?pageId=4424612
  *
  * 原有统计逻辑
  * 【各个频道的分析sql是分布在资源调度平台上，需整理到一起，分析共性】
  *
  * 需要做：
  *       1.解析出列表页维度  一级入口，二级入口
  *       2.对3.x日志的筛选和搜索入口，需要对日志格式做特殊处理
  *         pathMain like '%movie-search%'     为3.x的搜索入口
  *         pathMain like '%movie-retrieval%'  为3.x的筛选入口
  *
  * 使用事实表：
  *           2.x playview and 3.x play 原始日志生成分析用playview日志
  * 使用维度表：
  *           站点树维度表   dim_medusa_source_site [关联此表做过滤]
  *           2.x 使用second_category_code字段关联，获得二级入口的中文名字，以及过滤脏字段
  *           3.x 使用second_category字段关联，过滤脏字段

  * 使用的分析字段（从事实表获得）:
  * userId           度量值
  * path             解析出一级入口，二级入口  维度值
  * pathMain         解析出一级入口，二级入口  维度值
  *
  */
object MovieChannelClassificationStat extends BaseClass {
  private val tableName = "medusa_channel_eachtab_play_movie_info"
  private val fields = "day,channelname,tabname,play_user,play_num"
  private val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?,?)"
  private val deleteSql = s"delete from $tableName where day = ? "
  private val playNumLimit = 5000
  private val analyse_source_data_df_name = "movie_channel_classification_analyse_source_data_df"
  private val analyse_result_df_name = "movie_channel_classification_analyse_result_df"
  private val isDebug = true

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        println("---------------------" + ReadConfig.getConfig)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        sqlContext.udf.register("getListCategoryMedusa", PathParser.getListCategoryMedusaETL _)
        sqlContext.udf.register("getListCategoryMoretv", PathParser.getListCategoryMoretvETL _)
        //引入维度表
        val dimension_source_site_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SOURCE_SITE)
        val dimensionSourceSiteFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_source_site_input_dir)
        println(s"--------------------dimensionSourceSiteFlag is ${dimensionSourceSiteFlag}")
        if (dimensionSourceSiteFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SOURCE_SITE).registerTempTable(DimensionTypes.DIM_MEDUSA_SOURCE_SITE)
        } else {
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

          /** 最终此逻辑会合并进入事实表的ETL过程-start */
          /** 事实表数据处理步骤
            * 1.过滤单个用户播放单个视频量过大的情况
            * 2.关联站点树维度表过滤掉乱码的一级入口，二级入口
            */
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, date)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, date)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)
          if (medusaFlag && moretvFlag) {
            val medusa_table_df = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, date)
            medusa_table_df.cache()
            medusa_table_df.registerTempTable("medusa_table")
            val moretv_table_df = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.PLAYVIEW, date)
            medusa_table_df.cache()
            moretv_table_df.registerTempTable("moretv_table")

            /**
              * step1 解析出列表页维度，一级入口，二级入口 */
            /** 3.x 使用站点树维度表对3.x的二级入口进行过滤，防止日志里脏数据 */
            sqlStr =
              s"""
                 |select userId,
                 |       videoSid,
                 |       pathMain,
                 |       event,
                 |       getListCategoryMedusa(pathMain,1) as main_category,
                 |       getListCategoryMedusa(pathMain,2) as second_category,
                 |       '$MEDUSA'   as flag
                 |from medusa_table
                     """.stripMargin
            println("--------------------" + sqlStr)
            val movie_medusa_table_step2_df = sqlContext.sql(sqlStr)
            movie_medusa_table_step2_df.cache()
            movie_medusa_table_step2_df.registerTempTable("medusa_table_step2")
            writeToHDFSForCheck(date, "movie_medusa_table_step2_df", movie_medusa_table_step2_df, p.deleteOld)
            sqlStr =
              s"""
                 |select a.userId,
                 |       a.videoSid,
                 |       a.pathMain,
                 |       a.event,
                 |       a.main_category,
                 |       b.second_category,
                 |       a.flag
                 |from medusa_table_step2                       a
                 |join
                 |    ${DimensionTypes.DIM_MEDUSA_SOURCE_SITE}  b
                 |    on a.main_category=b.site_content_type and a.second_category=b.second_category
                     """.stripMargin
            println("--------------------" + sqlStr)
            val medusa_table_rdd = sqlContext.sql(sqlStr).toJSON
            medusa_table_rdd.cache()

            /** 2.x 使用站点树维度表对2.x的二级入口进行过滤，英文转中文 */
            sqlStr =
              s"""
                 |select userId,
                 |       videoSid,
                 |       path,
                 |       event,
                 |       getListCategoryMoretv(path,1)  as main_category,
                 |       getListCategoryMoretv(path,2)  as second_category,
                 |       '$MORETV'   as flag
                 |from moretv_table
                     """.stripMargin
            println("--------------------" + sqlStr)
            val movie_moretv_table_step2_df = sqlContext.sql(sqlStr)
            movie_moretv_table_step2_df.cache()
            movie_moretv_table_step2_df.registerTempTable("moretv_table_step2")
            writeToHDFSForCheck(date, "movie_moretv_table_step2_df", movie_moretv_table_step2_df, p.deleteOld)
            sqlStr =
              s"""
                 |select a.userId,
                 |       a.videoSid,
                 |       a.path,
                 |       a.event,
                 |       a.main_category,
                 |       b.second_category,
                 |       a.flag
                 |from moretv_table_step2                       a
                 |join
                 |    ${DimensionTypes.DIM_MEDUSA_SOURCE_SITE}  b
                 |    on a.main_category=b.site_content_type and a.second_category=b.second_category_code
                     """.stripMargin
            println("--------------------" + sqlStr)
            val moretv_table_rdd = sqlContext.sql(sqlStr).toJSON
            moretv_table_rdd.cache()

            //scheme merge
            val mergerRDD = medusa_table_rdd.union(moretv_table_rdd)
            mergerRDD.cache()
            val step1_table_df = sqlContext.read.json(mergerRDD)
            step1_table_df.cache()
            step1_table_df.registerTempTable("step1_table")
            writeToHDFSForCheck(date, "movie_cc_step1_table_df", step1_table_df, p.deleteOld)

            /** step2 用于过滤单个用户播放当个视频量过大的情况 */
            sqlStr =
              s"""
                 |select concat(userId,videoSid) as filterColumn
                 |from step1_table
                 |group by concat(userId,videoSid)
                 |having count(1)>=$playNumLimit
                     """.stripMargin
            println("--------------------" + sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("step2_table_filter")

            sqlStr =
              s"""
                 |select a.userId,
                 |       a.event,
                 |       a.main_category,
                 |       a.second_category
                 |from step1_table           a
                 |     left join
                 |     step2_table_filter    b
                 |     on concat(a.userId,a.videoSid)=b.filterColumn
                 |where b.filterColumn is null
                     """.stripMargin
            println("--------------------" + sqlStr)
            val step2_table_df = sqlContext.sql(sqlStr)
            step2_table_df.cache()
            step2_table_df.registerTempTable(analyse_source_data_df_name)
            writeToHDFSForCheck(date, analyse_source_data_df_name, step2_table_df, p.deleteOld)
          } else {
            throw new RuntimeException("2.x or 3.x log data is not exist")
          }

          /** 最终此逻辑会合并进入事实表的ETL过程-end */

          /** 进入分析代码，以后分析脚本编写、HUE查询、kylin查询只需要编写如下sql */
          sqlStr =
            s"""
               |select  second_category           as tabname,
               |        count(distinct userId)    as playUser,
               |        count(userId)             as playNum
               |from $analyse_source_data_df_name
               |where event in ('$MEDUSA_EVENT_START_PLAY','$MORETV_EVENT_START_PLAY') and
               |      main_category='$CHANNEL_MOVIE'
               |group by second_category
                   """.stripMargin
          println("--------------------" + sqlStr)
          val mysql_result_df = sqlContext.sql(sqlStr)
          writeToHDFSForCheck(date, analyse_result_df_name, mysql_result_df, p.deleteOld)
          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }
          //day,channelname,tabname,play_user,play_num
          mysql_result_df.collect.foreach(row => {
            util.insert(sqlInsert, sqlDate, CHANNEL_MOVIE, row.getString(0), new JLong(row.getLong(1)), new JLong(row.getLong(2)))
          })
        })
      }
      case None => {
      }
    }
  }

  //用来写入HDFS，测试数据是否正确
  def writeToHDFSForCheck(date: String, logType: String, df: DataFrame, isDeleteOld: Boolean): Unit = {
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

/**
  * 比对逻辑：
  * 1.首先比对movie频道下的分类个数，
  * 2.比对各个分类下的数据差异，找差异比较大的记录，然后根据写在HDFS的中间结果进行分析
  * */

/**遇到的问题
  *
  * */