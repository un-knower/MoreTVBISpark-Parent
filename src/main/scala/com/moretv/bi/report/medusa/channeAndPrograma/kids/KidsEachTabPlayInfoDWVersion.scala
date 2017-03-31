package com.moretv.bi.report.medusa.channeAndPrograma.kids

/**
  * Created by witnes on 1/11/17.
  */
import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.logETL.KidsPathParser
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, ParamsParseUtil}
import org.apache.spark.sql.DataFrame


/**
  * Created by witnes on 8/22/16.
  * 统计少儿频道各列表页(三级目录)的播放量
  */
object KidsEachTabPlayInfoDWVersion extends BaseClass {

  private val analyse_source_data_df_name = "channel_classification_analyse_source_data_df"
  private val playNumLimit = 5000
  private val tableName = "medusa_channel_eachtabwithgroup_play_kids_info"

  /*删除指定一天的所有记录*/
  private val deleteSql = s"delete from $tableName where day = ?"

  private val insertSql = s"insert into $tableName(day, channelname, groupname,tabname,play_num,play_user)" +
    "values (?,?,?,?,?,?)"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    sqlContext.udf.register("getListCategoryMedusa", KidsPathParser.pathMainParse _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE).
          registerTempTable(DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE)
        DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP).
          registerTempTable(DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var sqlStr = ""
        (0 until p.numOfDays).foreach(i => {

          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          /** 最终此逻辑会合并进入事实表的ETL过程-start */
          /** 事实表数据处理步骤
            * 1.过滤单个用户播放单个视频量过大的情况
            */
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, date)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          if (medusaFlag) {
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, date).registerTempTable("medusa_table")

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
                 |       getListCategoryMedusa(pathMain,3) as third_category,
                 |       b.program_code
                 |from medusa_table as a
                 |join ${DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP} as b
                 |on getListCategoryMedusa(a.pathMain,2) = b.path_code
                 |where getListCategoryMedusa(a.pathMain,1) = 'kids'
                     """.stripMargin
            sqlContext.sql(sqlStr).registerTempTable("step1_table_tmp")

            sqlStr =
              s"""
                |select a.userId,
                |       a.videoSid,
                |       a.event,
                |       a.main_category,
                |       a.program_code,
                |       a.third_category,
                |       b.area_name
                |from step1_table_tmp as a
                |join ${DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE} as b
                |on a.program_code = b.area_code
              """.stripMargin
            val step1_table_df = sqlContext.sql(sqlStr).cache()
            step1_table_df.registerTempTable("step1_table")

            /** step2 用于过滤单个用户播放当个视频量过大的情况 */
            sqlStr =
              s"""
                 |select concat(userId,videoSid) as filterColumn
                 |from step1_table
                 |group by concat(userId,videoSid)
                 |having count(1)>=$playNumLimit
                     """.stripMargin
            sqlContext.sql(sqlStr).registerTempTable("step2_table_filter")

            sqlStr =
              s"""
                 |select a.userId,
                 |       a.event,
                 |       a.main_category,
                 |       a.area_name,
                 |       a.third_category
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
          } else {
            throw new RuntimeException("3.x log data is not exist")
          }

          /** 最终此逻辑会合并进入事实表的ETL过程-end */
          sqlStr =
            s"""
              |select area_name,third_category,count(distinct userId) as platUser,count(userId) as playNum
              |from $analyse_source_data_df_name as a
              |where event = 'startplay'
              |group by area_name,third_category
            """.stripMargin
          val mysql_result_df = sqlContext.sql(sqlStr)


          /*将统计结果集插入Mysql */
          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          mysql_result_df.map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3))).collect().foreach(e => {
            try {
              util.insert(insertSql, sqlDate, "少儿", e._1, e._2,
                new JLong(e._4), new JLong(e._3))
            } catch {
              case e: Exception => println(e)
            }
          })
        })

      }
      case None => {
        throw new RuntimeException("at least needs one param: startDate")
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