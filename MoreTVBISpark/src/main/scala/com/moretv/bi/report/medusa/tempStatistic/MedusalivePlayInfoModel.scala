package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/4/24.
 */
object MedusalivePlayInfoModel extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("bi")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/medusa/parquet/$dateTime/live"

          val df = sqlContext.read.load(inputPath).select("liveType","apkSeries","userId","productModel","event","duration")
          val day = DateFormatUtils.toDateCN(dateTime)
          /*logType,date,apkSeries,userId,productModel,duration*/
          val logRdd = df.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getLong(5)))
          val filterRdd = logRdd.filter(_._2!=null).filter(_._2.take(16)=="MoreTV_TVApp3.0_").filter(_._1=="live")
          /*Getting live play_num and aver_duration*/
          val live_click_num = filterRdd.filter(_._3!=null).map(e=>e._3).count()
          val live_play_num = filterRdd.filter(_._5!=null).filter(_._5=="startplay").filter(_._3!=null).map(e=>e._3).count()
          val live_user_num = filterRdd.filter(_._5!=null).filter(_._5=="startplay").filter(_._3!=null).map(e=>e._3).distinct().count()
          val live_duration = filterRdd.filter(_._5!=null).filter(_._5=="switchchannel").filter(_._6!=null).map(e=>e._6)
            .filter(e=>{e<14400 && e>0}).reduce((x,y)=>x+y)
          val insertSql = "insert into medusa_live_play_info(date,click_num,play_num,user_num,total_duration) values (?,?," +
            "?,?,?)"
          util.insert(insertSql,day,new JLong(live_click_num),new JLong(live_play_num),new JLong(live_user_num),new JLong
          (live_duration))






          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }



  }
}
