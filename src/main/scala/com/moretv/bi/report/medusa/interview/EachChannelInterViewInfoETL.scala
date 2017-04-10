package com.moretv.bi.report.medusa.interview

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.SQLContext

/**
  * 创建人：吴久林
  * 创建时间：2017/4/6
  * 程序作用：分析各个频道的浏览次数与人数
  * 数据输入：
  * 数据输出：
  */
object EachChannelInterViewInfoETL extends BaseClass  {
  private val fields = "day,channel,view_user,view_num"
  private val channel_to_mysql_table=Map(
    CHANNEL_KIDS->"medusa_channel_view_kids_info",
    CHANNEL_OPERA->"medusa_channel_view_xiqu_info",
    CHANNEL_COMIC->"medusa_channel_view_comic_info",
    CHANNEL_VARIETY_PROGRAM->"medusa_channel_view_zongyi_info",
    CHANNEL_HOT->"medusa_channel_view_hot_info",
    CHANNEL_MV->"medusa_channel_view_mv_info",
    CHANNEL_RECORD->"medusa_channel_view_jilu_info",
    CHANNEL_TV->"medusa_channel_view_tv_info",
    CHANNEL_MOVIE->"medusa_channel_view_movie_info"
  )
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }
  override def execute(args: Array[String]): Unit ={
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(w => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.INTERVIEW_ETL, date).registerTempTable("interview_etl")

          //删除历史记录
          val channelArray=Array(CHANNEL_KIDS,CHANNEL_OPERA,CHANNEL_COMIC,CHANNEL_VARIETY_PROGRAM,CHANNEL_HOT,CHANNEL_MV,CHANNEL_RECORD,CHANNEL_TV,CHANNEL_MOVIE)
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
//            val channelLike= "'"+channel_name+"%'"
            val sql=
              s"""
                 | select count(distinct userId) as view_user ,count(userId) as view_num
                 | from interview_etl
                 | where contentType='$channel_name' and event='enter'
             """.stripMargin

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
