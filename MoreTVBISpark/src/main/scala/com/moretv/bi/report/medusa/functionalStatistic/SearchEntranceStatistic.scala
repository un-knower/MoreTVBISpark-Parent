package com.moretv.bi.report.medusa.functionalStatistic

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 9/18/16.
  */

/**
  * 需求: 功能统计 - 搜索 - 有效搜索评估 - 个入口占比
  * 日志: /log/medusa/parquet/loadDate/clickEntrance
  * 输出: day,search_entrance,click_count
  */

object SearchEntranceStatistic extends BaseClass{

  private val tableName = "search_entrance_freq"

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //init & util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w=>{

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

         /* //path
          val loadPath = s"/log/medusa/parquet/$loadDate/clickEntrance"
          */

          //df
          var df:DataFrame = null
          try{
          /*  df =sqlContext.read.parquet(loadPath).select("logType","userId","params['entrance']")
              .filter("logType='event'").filter("params['entrance'] is not null")*/
            df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.CLICK_ENTRANCE,loadDate).select("logType","userId","params['entrance']")
              .filter("logType='event'").filter("params['entrance'] is not null")
          }catch {
            case ex:Exception =>{
              println(ex)
              System.exit(-1)
            }

          }

          //rdd
          val rdd = df.map(e=>(e.getString(2),e.getString(1))).cache

          //aggregate
          val pvMap = rdd.countByKey

          //deal with table
          if(p.deleteOld){
            util.delete(s"delete from $tableName where day =$sqlDate")
          }

          pvMap.foreach(w=>{
            try{
              util.insert(s"insert into $tableName(day,search_entrance,click_count)values(?,?,?)",
                sqlDate,w._1,new JLong(w._2))
            }catch {
              case ex:Exception =>{
                println(ex)
                System.exit(-1)
              }
            }
          })

        })

      }
      case None => {
        throw new Exception("SearchEntranceStatistic fail ")
      }
    }
  }
}
