package com.moretv.bi.report.medusa.subject

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.SQLContext

/**
  * 创建人：郭浩
  * 创建时间：2017/4/6
  * 程序作用：分析各个频道专题的浏览播放量及用户数
  * 数据输入：
  * 数据输出：
  */
object EachChannelSubjectViewInfoETL extends BaseClass  {
  private val fields = "day,channelname,view_user,view_num"
  private val channel_to_mysql_table=Map(
    CHANNEL_COMIC->"medusa_channel_subject_view_comic_info",
    CHANNEL_MOVIE->"medusa_channel_subject_view_movie_info",
    CHANNEL_TV->"medusa_channel_subject_view_tv_info",
    CHANNEL_HOT->"medusa_channel_subject_view_hot_info",
    CHANNEL_VARIETY_PROGRAM->"medusa_channel_subject_view_zongyi_info",
    CHANNEL_OPERA->"medusa_channel_subject_view_xiqu_info",
    CHANNEL_RECORD->"medusa_channel_subject_view_jilu_info",
    CHANNEL_KIDS->"medusa_channel_subject_view_kids_info")
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }
  override def execute(args: Array[String]): Unit ={
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        //引入维度表
        val dimension_subject_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        if (dimensionSubjectFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
        } else {
          throw new RuntimeException(s"--------------------dimension not exist")
        }

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(w => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.SUBJECT_INTERVIEW_ETL, date).registerTempTable("suject_interview_table")

          //删除历史记录
          val channelArray=Array(CHANNEL_MOVIE,CHANNEL_COMIC,CHANNEL_TV,CHANNEL_HOT,CHANNEL_VARIETY_PROGRAM,CHANNEL_OPERA,CHANNEL_RECORD,CHANNEL_KIDS)
          if (p.deleteOld) {
            for(channel_name <-channelArray){
              val tableName=channel_to_mysql_table.get(channel_name).get
              val deleteSql = s"delete from $tableName where day = ? "
              util.delete(deleteSql, sqlDate)
            }
          }
          //业务分析
          for(channel_name <-channelArray){
            val tableName=channel_to_mysql_table.get(channel_name).get
            val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?)"


            val sql=
              s"""
                 |select count(distinct a.userId) as view_user ,
                 |	count(a.userId) as view_num
                 |from
                 |	(select userId,subjectCode from suject_interview_table where event in ('enter','view')   ) a
                 |join
                 |	(select * from ${DimensionTypes.DIM_MEDUSA_SUBJECT} where dim_invalid_time is null)  b
                 |	on a.subjectCode = b.subject_code and b.subject_content_type = "${channel_name}"
             """.stripMargin
            println(sql)

          sqlContext.sql(sql).collect.foreach(row=>{
              val channelname = channel_name
              val view_user = new JLong(row.getLong(0))
              val view_num = new JLong(row.getLong(1))
              util.insert(sqlInsert,sqlDate,channelname,view_user,view_num)
            })

          }

        })
      }
      case None => {
      }
    }
  }

}
