package com.moretv.bi.report.medusa.functionalStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/5/16.
 * 统计搜索节目信息
 */
object SearchProgramInfo extends BaseClass{
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
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          //val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

        /*  val playviewInput = s"$medusaDir/$date/{playview,detail}/"

          sqlContext.read.parquet(playviewInput).select("userId","path","pathMain","event","videoSid")
            .registerTempTable("log_data")

          val searchRdd = sqlContext.sql("select videoSid,count(userId),count(distinct userId)" +
            " from log_data where event in ('startplay','playview','view') and (path like '%-search%' or " +
            "pathMain like '%-search%') group by videoSid").map(e=>(e.getString(0),e.getLong(1),e
            .getLong(2)))*/

          //
          val df1=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).select("userId","path","pathMain","event","videoSid")
          val df2=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MERGER,LogTypes.DETAIL,date).select("userId","path","pathMain","event","videoSid")
          val df =df1 unionAll df2
          df.registerTempTable("log_data")
          val searchRdd = df.sqlContext.sql("select videoSid,count(userId),count(distinct userId)" +
            " from log_data where event in ('startplay','playview','view') and (path like '%-search%' or " +
            "pathMain like '%-search%') group by videoSid").map(e=>(e.getString(0),e.getLong(1),e
            .getLong(2)))

          val insertSql="insert into medusa_function_statistic_search_info(day,video_sid,title,play_num,play_user) " +
            "values (?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql="delete from medusa_function_statistic_search_info where day=?"
            util.delete(deleteSql,insertDate)
          }


          searchRdd.collect().foreach(e=>{
            try{
              util.insert(insertSql,insertDate,e._1,ProgramRedisUtil.getTitleBySid(e._1),new JLong(e._2),new JLong(e
                ._3))
            }catch {
              case e:Exception => throw e
            }
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
