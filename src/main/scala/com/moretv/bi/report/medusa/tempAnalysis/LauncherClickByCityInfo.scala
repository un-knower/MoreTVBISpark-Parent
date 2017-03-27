package com.moretv.bi.report.medusa.tempAnalysis

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{CodeToNameUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by 陈佳星 on 2016/8/31.
  */
object LauncherClickByCityInfo extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    sqlContext.udf.register("getCity", IPLocationDataUtil.getCity _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        //val appRecommendDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          val df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.HOMEACCESS,date)
           df.registerTempTable("log_data")
          val rdd = df.sqlContext.sql("select accessArea,accessLocation,count(distinct userId),count(userId) from log_data " +
            s"where event='openapp' group by appSid ").map(e => (e.getString(0), e.getLong(1), e.getLong(2)))

          if(p.deleteOld){
            val deleteSql="delete from medusa_app_recommend_open_info where day=?"
            util.delete(deleteSql,insertDate)
          }


          val sqlInsert = "insert into medusa_app_recommend_open_info(day,appSid,appName,user_num,play_num) " +
            "values (?,?,?,?,?)"

          rdd.collect().foreach(e => {
            util.insert(sqlInsert, insertDate, e._1,CodeToNameUtils.getApplicationNameBySid(e._1),new JLong(e._2), new JLong(e._3))
          })
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
