package com.moretv.bi.report.medusa.functionalStatistic

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/18/16.
  */

/**
  *
  */
object SearchEntranceResultStatistic extends BaseClass{

  private val tableName = "search_entrance_result_freq"

  def main(args: Array[String]) {

    ModuleClass.executor(SearchEntranceResultStatistic,args)

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

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/clickResult"

          //df
          val df = sqlContext.read.parquet(loadDate).select("logType","eventId","userId","entrance")
                      .filter("logType ='event'").filter("eventId='medusa-search-clickResult'")
          //rdd
          val rdd = df.map(e=>(e.getString(3),e.getString(2)))

          //aggregate
          val sumMap = rdd.countByKey

          //deal with table
          util.delete(s"delete from $tableName where day = ?",sqlDate)

          sumMap.foreach(w=>{

            try{

              util.insert(s"insert into $tableName(day,search_entrance,result_count)values(?,?,?)",
                sqlDate,w._1,new JLong(w._2))

            }catch {
              case ex:Exception => throw new Exception(ex)
            }

          })

        })
      }
      case None => {
        throw new Exception("SearchEntranceResultStatistic fails")
      }

    }
  }
}
