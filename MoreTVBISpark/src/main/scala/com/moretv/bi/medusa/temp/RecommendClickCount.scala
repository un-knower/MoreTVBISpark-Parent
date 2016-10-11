package com.moretv.bi.medusa.temp

import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import java.lang.{Long => JLong}
/**
  * Created by witnes on 9/12/16.
  */

/**
  * 统计一段时间内homeaccess点击的pv 和 uv
  */
object RecommendClickCount extends BaseClass{

  private val tableName = "medusa_recommend_version_timed_click"

  private val filterStr = "comic13,movie886,movie871"


  def main(args: Array[String]):Unit = {

    ModuleClass.executor(RecommendClickCount, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        //date
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          //date
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val date1 = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val readHomeAccess = s"/log/medusa/parquet/$date/homeaccess"
          println(readHomeAccess)

          //df
          sqlContext.read.parquet(readHomeAccess).registerTempTable("log1")

          val dfClick =
            sqlContext.sql("select date, apkVersion, userId ,datetime from log1 where accessArea='recommendation' and locationIndex=5")
              .filter(s"date='$date1'")
              .filter("apkVersion in ('3.0.8','3.0.9') " )
              .filter(s"datetime > '$date1 09:00:00'")

          //rdd
          val rddClick = dfClick.map(e => ((e.getString(0), e.getString(1)), e.getString(2))).cache

          val clickPv = rddClick.countByKey

          val clickUv = rddClick.distinct.countByKey

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day = ? ", date1)
          }

          clickPv.foreach(i=>{

            val key = i._1

            val c_uv = clickUv.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            val c_pv = clickPv.get(key) match {
              case Some(e) => e
              case _ => 0L
            }

            println(key._1,key._2,c_pv,c_uv)

            try{

              util.insert(s"insert into $tableName (day,apkVersion,click_pv,click_uv) " +
                s"values (?,?,?,?)", key._1,key._2,new JLong(c_pv),new JLong(c_uv)
               )

            }catch {
              case ex:Exception => {
                println(ex)
                throw new RuntimeException("sql fail")
              }
            }

          })


        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }



  }


}
