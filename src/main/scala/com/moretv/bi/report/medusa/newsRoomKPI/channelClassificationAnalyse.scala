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
  * 需要做：解析出列表页维度  一级入口，二级入口
  * 使用的事实表：
  * 解析出列表页维度  一级入口，二级入口
  * 使用的维度表：
  * 节目维度表           dim_medusa_source_site [关联此表做过滤]

  * 使用的分析字段（从事实表获得）:
  * userId           度量值
  * path             解析出一级入口，二级入口  维度值
  * pathMain         解析出一级入口，二级入口  维度值
  *
  */
object channelClassificationAnalyse extends BaseClass {
  private val tableName = "medusa_channel_eachtab_play_movie_info"
  private val fields = "day,channelname,tabname,play_user,play_num"
  private val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?,?)"
  private val deleteSql = s"delete from $tableName where day = ? "
  private val playNumLimit = 5000
  private val analyse_source_data_df_name = "channel_classification_analyse_source_data_df"
  private val analyse_result_df_name = "channel_classification_analyse_result_df"

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
            */
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, date)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, date)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)
          if (medusaFlag && moretvFlag) {
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, date).registerTempTable("medusa_table")
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.PLAYVIEW, date).registerTempTable("moretv_table")

            /**
              * step1 解析出列表页维度  一级入口，二级入口 */
            sqlStr =
              s"""
                 |select userId,
                 |       videoSid,
                 |       pathMain,
                 |       event,
                 |       getListCategoryMedusa(pathMain,1) as main_category,
                 |       getListCategoryMedusa(pathMain,2) as sub_category,
                 |       '$MEDUSA'   as flag
                 |from medusa_table
                     """.stripMargin
            println("--------------------" + sqlStr)
            val medusa_table_rdd = sqlContext.sql(sqlStr).toJSON

            sqlStr =
              s"""
                 |select userId,
                 |       videoSid,
                 |       path,
                 |       event,
                 |       getListCategoryMoretv(path,1)  as main_category,
                 |       getListCategoryMoretv(path,2)  as sub_category,
                 |       '$MORETV'   as flag
                 |from moretv_table
                     """.stripMargin
            val moretv_table_rdd = sqlContext.sql(sqlStr).toJSON
            val mergerRDD = medusa_table_rdd.union(moretv_table_rdd)
            val step1_table_df = sqlContext.read.json(mergerRDD)
            step1_table_df.cache()
            step1_table_df.registerTempTable("step1_table")
            writeToHDFSForCheck(date, "cc_step1_table_df", step1_table_df, p.deleteOld)

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
            println("--------------------" + sqlStr)
            val step2_table_df = sqlContext.sql(sqlStr)
            step2_table_df.cache()
            step2_table_df.registerTempTable(analyse_source_data_df_name)
            writeToHDFSForCheck(date, analyse_source_data_df_name, step2_table_df, p.deleteOld)
          } else {
            throw new RuntimeException("2.x or 3.x log data is not exist")
          }
          /** 最终此逻辑会合并进入事实表的ETL过程-end */

          /** 进入分析代码 */
          sqlStr =
            s"""
               |select  sub_category            as tabname,
               |        count(userId)             as playNum,
               |        count(distinct userId)    as playUser
               |from $analyse_source_data_df_name
               |where event in ('$MEDUSA_EVENT_START_PLAY','$MORETV_EVENT_START_PLAY') and
               |      main_category='$CHANNEL_MOVIE'
               |group by sub_category
                   """.stripMargin
          println("--------------------" + sqlStr)
          val mysql_result_df = sqlContext.sql(sqlStr)
          writeToHDFSForCheck(date, analyse_result_df_name, mysql_result_df, p.deleteOld)
          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }
          //day,channelname,tabname,play_user,play_num
          mysql_result_df.collect.foreach(row => {
            util.insert(sqlInsert, sqlDate, row.getString(0), row.getString(1), row.getString(2),new JLong(row.getLong(3)), new JLong(row.getLong(4)))
          })
        })
      }
      case None => {
      }
    }
  }

  //用来写入HDFS，测试数据是否正确
  def writeToHDFSForCheck(date: String, logType: String, df: DataFrame, isDeleteOld: Boolean): Unit = {
    println(s"--------------------$logType is write done.")
    val outputPath = DataIO.getDataFrameOps.getPath(MERGER, logType, date)
    if (isDeleteOld) {
      HdfsUtil.deleteHDFSFile(outputPath)
    }
    df.write.parquet(outputPath)
  }


}