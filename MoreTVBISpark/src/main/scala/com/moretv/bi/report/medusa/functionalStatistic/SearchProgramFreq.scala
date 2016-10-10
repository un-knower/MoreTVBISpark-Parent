package com.moretv.bi.report.medusa.functionalStatistic

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/14/16.
  */
object SearchProgramFreq extends BaseClass{

  private val tableName = "medusa_searchprogram_freq"

  def main(args: Array[String]): Unit ={

      ModuleClass.executor(SearchProgramFreq,args)

  }
  override def execute(args: Array[String]): Unit = {

      ParamsParseUtil.parse(args) match {
        case Some(p) => {
          //util init
          val util = new DBOperationUtils("medusa")
          val startDate = p.startDate
          val cal = Calendar.getInstance
          cal.setTime(DateFormatUtils.readFormat.parse(startDate))

          (0 until p.numOfDays).foreach(w =>{
            //date
            val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
            cal.add(Calendar.DAY_OF_MONTH,-1)
            val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

            //path
            val path = s"/log/medusa/parquet/$loadDate/clickResult"
            println(path)
            //df
            val df = sqlContext.read.parquet(path).select("userId","contentType")
                        .filter("contentType is not null")
            //rdd
            val rdd = df.map(e=>(e.getString(1),e.getString(0)))

            //aggregate
            val sumRdd = rdd.countByKey
            if(p.deleteOld){
              util.delete(s"delete from $tableName where day = ? ",sqlDate)
            }
            //deal with table
            sumRdd.foreach(w=>{
              util.insert(s"insert into $tableName(day,contentType,search_num)values(?,?,?)",
                sqlDate,w._1,new JLong(w._2))
            })
          })
        }
        case None => {
          throw new Exception("at least one param for SearchProgramFreq")
        }
      }

  }
}
