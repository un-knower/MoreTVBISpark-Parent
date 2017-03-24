package com.moretv.bi.report.medusa.functionalStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by 陈佳星 on 2016/8/31.
  */
object appRecommendOpenInfo extends BaseClass{

  val tableName = "medusa_app_recommend_open_info"

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          val df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MERGER,LogTypes.APP_RECOMMEND,date)
           df.registerTempTable("log_data")

          DataIO.getDataFrameOps.getDimensionDF(
            sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APPLICATION
          ).registerTempTable("dim_application")

          val rdd = df.sqlContext.sql("select a.appSid, count(distinct a.userId), count(a.userId), first(b.application_name)"
            + " from log_data a left join dim_application b on a.appSid = b.application_sid and b.dim_invalid_time is null"
            + s" where a.event='openapp' group by a.appSid ").map(
            e => (e.getString(0), e.getLong(1), e.getLong(2), e.getString(3))
          )

          if(p.deleteOld){
            val deleteSql=s"delete from $tableName where day=?"
            util.delete(deleteSql,insertDate)
          }


          val sqlInsert = s"insert into $tableName(day,appSid,appName,user_num,play_num) " +
            "values (?,?,?,?,?)"

          rdd.collect().foreach(e => {
            util.insert(sqlInsert, insertDate, e._1, e._4,new JLong(e._2), new JLong(e._3))
          })
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
