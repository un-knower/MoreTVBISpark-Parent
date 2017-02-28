package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.{Long => JLong}

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by laishun on 15/10/9.
 */
object Live_time extends BaseClass with DateUtil{

  def main(args: Array[String]): Unit = {
    config.setAppName("Live_time")
    ModuleClass.executor(Live_time,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{


        val path = "/mbi/parquet/live/"+p.startDate
        val df = sqlContext.read.load(path)
        val resultRDD = df.select("date","channelSid","duration").map(e =>(e.getString(0),e.getString(1),e.getInt(2).toLong)).
            map(e=>(getKeys(e._1,e._2),e._3)).filter(x => {x._2 > 0 && x._2 < 100000}).reduceByKey((x,y)=>x+y).collect()

        val util = new DBOperationUtils("eagletv")
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from live_time where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO live_time(year,month,day,live_channel,seconds) VALUES(?,?,?,?,?)"
        resultRDD.foreach(x =>{
          var title = CodeToNameUtils.getChannelNameBySid(x._1._4)
          if(title == null) title = x._1._4
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,title,new JLong(x._2))
        })

      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, channelSid:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    (year,month,date,channelSid)
  }
}
