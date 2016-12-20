package com.moretv.bi.report.medusa.functionalStatistic

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/19/16.
  */

/**
  * 数据源: 选择搜索结果日志 (clickResult)
  * 提取特征: entrance, params['contentType']
  * 输出路径: tbl[ medusa.search_entrance_contenttype_stat]
  */
object SearchEntranceContentTypeResultFreq extends BaseClass{

  private val tableName =  "search_entrance_contenttype_stat"


  def main(args: Array[String]) {

    ModuleClass.executor(this, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //init & util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        //date
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w=>{
          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

        /*  //path
          val loadPath = s"/log/medusa/parquet/$loadDate/clickResult"

          //df
          sqlContext.read.parquet(loadPath).registerTempTable("log")
          val df = sqlContext.sql("select params['contentType'], userId from log " +
            "where logType='event' and eventId='medusa-search-clickResult' and params['contentType'] is not null ")
*/
          val df1=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.CLICKSEARCHRESULT,loadDate)
          df1.registerTempTable("log")
          val df = df1.sqlContext.sql("select params['contentType'], userId from log " +
            "where logType='event' and eventId='medusa-search-clickResult' and params['contentType'] is not null ")

          //rdd
          val rdd = df.map(e=>(("launcher",e.getString(0)),e.getString(1)))

          //aggregate
          val pvMap = rdd.countByKey

          //deal with table
          if(p.deleteOld){
            util.delete(s"delete from $tableName where day=?", sqlDate)
          }

          pvMap.foreach(w=>{
            util.insert(s"insert into $tableName(day,search_entrance,contenttype,result_count)values(?,?,?,?)",
              sqlDate,w._1._1,w._1._2,new JLong(w._2) )
          })
        })

      }
      case None => {
        throw  new Exception("SearchEntranceContentTypeResultFreq fails")
      }
    }
  }
}
