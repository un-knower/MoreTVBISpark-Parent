package com.moretv.bi.medusa.temp

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
/**
 * Created by HuZhehua on 2016/4/12.
 */
//Medusa登录和退出日志统计
object MedusaEnterAndExitLogStatistics extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val path_enter = "/log/medusa/parquet/"+date+"/enter"
          val path_exit = "/log/medusa/parquet/"+date+"/exit"
          val enterDF = sqlContext.read.load(path_enter).select("userId").persist()
          val exitDF = sqlContext.read.load(path_exit).select("userId").persist()

          //Medusa中enter和exit的人数次数
          val mds_enter_pv = enterDF.count()
          val mds_enter_uv = enterDF.distinct().count()
          val mds_exit_pv = exitDF.count()
          val mds_exit_uv = exitDF.distinct().count()

          if(p.deleteOld){
            val sqlDelete = "DELETE FROM enterAndExitLog WHERE day = ?"
            util.delete(sqlDelete,day)
          }
          val sqlInsert = "INSERT INTO enterAndExitLog(day,tag,enter_pv,enter_uv,exit_pv,exit_uv) VALUES(?,?,?,?,?,?)"
          util.insert(sqlInsert,day,"medusa",new JLong(mds_enter_pv),new JLong(mds_enter_uv),new JLong(mds_exit_pv),new JLong(mds_exit_uv))

          cal.add(Calendar.DAY_OF_MONTH, -1)
          enterDF.unpersist()
          exitDF.unpersist()
        })
        util.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}

