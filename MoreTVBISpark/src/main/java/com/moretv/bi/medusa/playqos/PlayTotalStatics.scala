package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/9/16.
  */

/**
  * 计算 play日志 所有视频 uv
  */
object PlayTotalStatics extends  BaseClass{

  private  val tableName = "medusa_play_total"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(PlayTotalStatics, args)
  }

  override def execute(args: Array[String]): Unit =  {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = new DBOperationUtils("medusa")
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        var readPath =""
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val date1 = DateFormatUtils.cnFormat.format(cal.getTime)

          readPath = s"/log/medusa/parquet/$date/play"
          println(readPath)

          val rdd =sqlContext.read.parquet(readPath).select("date","userId","apkVersion","event")
              .filter("event='startplay'")
                .filter(s"date='$date1'")
                  .map(e=>((e.getString(0), e.getString(2)),e.getString(1)))


          val sumRdd = rdd.distinct.countByKey

          sumRdd.take(10).foreach(println)

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day =? ",date1)
          }

          sumRdd.foreach(w => {
            util.insert(s"insert into $tableName(day,apkVersion,num)values(?,?,?)",
              w._1._1,w._1._2,new JLong(w._2))
          })
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
