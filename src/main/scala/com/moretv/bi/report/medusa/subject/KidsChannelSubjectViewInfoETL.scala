package com.moretv.bi.report.medusa.subject

import java.util.Calendar
import java.lang.{Long => JLong}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.SQLContext

/**
  * 创建人：郭浩
  * 创建时间：2017/4/6
  * 程序作用：分析少儿频道的浏览播放量及用户数
  * 数据输入：
  * 数据输出：
  */
object KidsChannelSubjectViewInfoETL extends BaseClass  {
  private val tableName = "medusa_channel_view_kids_info "
  private val fields = "day,channel,view_user,view_num"
  private val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?)"
  private val deleteSql = s"delete from $tableName where day = ? "
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
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.INTERVIEW_ETL, date).registerTempTable("interview_kids_table")
          val sql=
            s"""
               | select count(distinct userId) as view_user ,count(userId) as view_num from interview_kids_table where contentType in ('kids','kids_home') and event='enter'
             """.stripMargin
          //删除历史记录
          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }
          sqlContext.sql(sql).collect.foreach(row=>{
            val channel = "kids"
            val view_user = new JLong(row.getLong(0))
            val view_num = new JLong(row.getLong(1))
            util.insert(sqlInsert,sqlDate,channel,view_user,view_num)
          })
        })
      }
      case None => {
      }
    }
  }

}
