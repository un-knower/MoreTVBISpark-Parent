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
  * Created by baozhi.wang on 2017/4/11.
  * 2.频道分类统计的播放次数播放人数统计【频道及栏目编排-频道分类统计-「电影,电视剧」-频道分类统计】
  * 逻辑方式【与ChannelClassificationStatETL代码比较】：
  *   使用事实表与维度表关联，直接获得分析结果
  *
  */
object ShortChannelClassificationStatETL extends BaseClass {
  private val fields = "day,channelname,tabname,play_user,play_num"
  private val analyse_source_data_df_name = "all_channel_classification_analyse_source_data_df"
  private val analyse_result_df_name = "all_channel_classification_analyse_result_df"
  private val isDebug = false
  private val channel_to_mysql_table=Map(
    CHANNEL_COMIC->"medusa_channel_eachtab_play_comic_info",
    CHANNEL_MOVIE->"medusa_channel_eachtab_play_movie_info",
    CHANNEL_TV->"medusa_channel_eachtab_play_tv_info",
    CHANNEL_HOT->"medusa_channel_eachtab_play_hot_info",
    CHANNEL_VARIETY_PROGRAM->"medusa_channel_eachtab_play_zongyi_info",
    CHANNEL_OPERA->"medusa_channel_eachtab_play_xiqu_info",
    CHANNEL_RECORD->"medusa_channel_eachtab_play_jilu_info")

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var sqlStr = ""
        (0 until p.numOfDays).foreach(w => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.PLAY_VIEW_ETL, date).registerTempTable(analyse_source_data_df_name)

          /** 进入分析代码，以后分析脚本编写、HUE查询、kylin查询只需要编写如下sql */
          sqlStr =
            s"""
               |select  main_category             as channelname,
               |        second_category           as tabname,
               |        count(distinct userId)    as playUser,
               |        count(userId)             as playNum
               |from $analyse_source_data_df_name
               |where event in ('$MEDUSA_EVENT_START_PLAY','$MORETV_EVENT_START_PLAY') and
               |      main_category in ('$CHANNEL_MOVIE','$CHANNEL_COMIC','$CHANNEL_TV','$CHANNEL_HOT','$CHANNEL_VARIETY_PROGRAM','$CHANNEL_OPERA','$CHANNEL_RECORD')
               |group by main_category,
               |         second_category
                   """.stripMargin
          println("--------------------" + sqlStr)

          val mysql_result_df = sqlContext.sql(sqlStr)
          writeToHDFSForCheck(date, analyse_result_df_name, mysql_result_df, p.deleteOld)
          if (p.deleteOld) {
            val channelArray=Array(CHANNEL_MOVIE,CHANNEL_COMIC,CHANNEL_TV,CHANNEL_HOT,CHANNEL_VARIETY_PROGRAM,CHANNEL_OPERA,CHANNEL_RECORD)
            for(channel_name <-channelArray){
              val tableName=channel_to_mysql_table.get(channel_name).get
              val deleteSql = s"delete from $tableName where day = ? "
              util.delete(deleteSql, sqlDate)
            }
          }
          //day,channelname,tabname,play_user,play_num
          mysql_result_df.collect.foreach(row => {
            val channel_name=row.getString(0)
            val tableName=channel_to_mysql_table.get(channel_name).get
            val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?,?)"
            util.insert(sqlInsert, sqlDate, channel_name, row.getString(1), new JLong(row.getLong(2)), new JLong(row.getLong(3)))
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

  /**遇到的问题
    *
    * 1.名作之壁 在站点树里解析为黑马之作
    * 黑马之作 包含两个部分，一个是2.x的名作之壁，另一个是3.x黑马之作
    *
    *
    * 2.在站点树里没有
    *  动漫： 名作之壁
    *  戏曲： 戏曲综艺
    *  记录： 猫叔推荐 、 军事风云
    *  综艺： 2016新歌声
    *  资讯： 资讯专题
    *  电视： 韩剧热流、粤语佳片、科学幻想
    *  电影： 已收藏、日韩亚太
    * */


}