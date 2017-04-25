package com.moretv.bi.report.medusa.playview

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.SQLContext

/**
  * 创建人：吴久林
  * 创建时间：2017/4/10
  * 程序作用：分析各个频道的播放总时长
  * 数据输入：
  * 数据输出：
  */
object EachChannelPlayViewDurationInfoETL extends BaseClass  {
  private val fields_mv = "day,channel,play_duration,user"
  private val fields_avg = "day,channel,play_duration,avg_duration"
  private val fields_other = "day,channel,play_duration"
  private val channel_to_mysql_table=Map(
    CHANNEL_KIDS->"medusa_channel_play_duration_kids_info",
    CHANNEL_OPERA->"medusa_channel_play_duration_xiqu_info",
    CHANNEL_COMIC->"medusa_channel_play_duration_comic_info",
    CHANNEL_VARIETY_PROGRAM->"medusa_channel_play_duration_zongyi_info",
    CHANNEL_HOT->"medusa_channel_play_duration_hot_info",
    CHANNEL_MV->"medusa_channel_play_duration_mv_info",
    CHANNEL_RECORD->"medusa_channel_play_duration_jilu_info",
    CHANNEL_TV->"medusa_channel_play_duration_tv_info",
    CHANNEL_MOVIE->"medusa_channel_play_duration_movie_info"
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
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.PLAYVIEW, date).registerTempTable(LogTypes.PLAYVIEW)

          //删除历史记录
          val channelArray = Array(CHANNEL_KIDS,CHANNEL_OPERA,CHANNEL_COMIC,CHANNEL_VARIETY_PROGRAM,CHANNEL_HOT,CHANNEL_MV,CHANNEL_RECORD,CHANNEL_TV,CHANNEL_MOVIE)
          if (p.deleteOld) {
            for(channel_name <-channelArray){
              val tableName = channel_to_mysql_table.get(channel_name).get
              val deleteSql = s"delete from $tableName where day = ? "
              util.delete(deleteSql, sqlDate)
            }
          }
          //业务分析
          for(channel_name <-channelArray){
            val tableName = channel_to_mysql_table.get(channel_name).get
            var sqlStr = ""
            var sqlSpark = ""
            if (channel_name == "mv"){
              sqlStr = s"insert into $tableName($fields_mv) values(?,?,?,?)"
              sqlSpark =
                s"""
                   | select sum(duration) as play_duration,
                   | count(distinct userId) as user
                   | from ${LogTypes.PLAYVIEW}
                   | where contentType ='$channel_name'
                   | and event in ('playview','userexit','selfend')
                   | and duration >= 0 and duration<=10800
             """.stripMargin
            }

            if (channel_name=="kids" || channel_name=="tv" || channel_name=="movie"){
              sqlStr = s"insert into $tableName($fields_avg) values(?,?,?,?)"
              sqlSpark =
                s"""
                   | select sum(duration) as play_duration,
                   | sum(duration)/count(distinct userId) as avg_duration
                   | from ${LogTypes.PLAYVIEW}
                   | where contentType ='$channel_name'
                   | and event in ('playview','userexit','selfend')
                   | and duration >= 0 and duration<=10800
             """.stripMargin
            }

            if (channel_name=="jilu" || channel_name=="comic" || channel_name=="xiqu" || channel_name=="zongyi" || channel_name=="hot"){
              sqlStr = s"insert into $tableName($fields_other) values(?,?,?)"
              sqlSpark =
                s"""
                   | select sum(duration) as play_duration
                   | from ${LogTypes.PLAYVIEW}
                   | where contentType ='$channel_name'
                   | and event in ('playview','userexit','selfend')
                   | and duration >= 0 and duration<=10800
             """.stripMargin
            }

           sqlContext.sql(sqlSpark).collect.foreach(row=>{
              if (channel_name=="jilu" || channel_name=="comic" || channel_name=="xiqu" || channel_name=="zongyi" || channel_name=="hot"){
                val play_duration = new JLong(row.getLong(0))
                util.insert(sqlStr,sqlDate,channel_name,play_duration)
              } else if (channel_name == "mv"){
                val play_duration = new JLong(row.getLong(0))
                val user = new JLong(row.getLong(1))
                util.insert(sqlStr,sqlDate,channel_name,play_duration,user)
              } else {
                val play_duration = new JLong(row.getLong(0))
                val avg_duration = row.getDouble(1)
                util.insert(sqlStr,sqlDate,channel_name,play_duration,avg_duration)
              }
            })

          }

        })
      }
      case None => {
      }
    }
  }

}
