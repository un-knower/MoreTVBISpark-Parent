package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/4/24.
 */
object livePlayInfoModel extends SparkSetting{
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
          val inputPath = s"/mbi/parquet/live/$dateTime/"

          val df = sqlContext.read.load(inputPath).select("logType","date","apkSeries","userId","productModel","duration")
          val day = DateFormatUtils.toDateCN(dateTime)
          /*logType,date,apkSeries,userId,productModel,duration*/
          val logRdd = df.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getInt(5)
            .toLong)).cache()
          val filterRdd = logRdd.filter(_._3.take(16)=="MoreTV_TVApp2.0_").filter(_._1=="live")
          /*Getting live play_num and aver_duration*/
          val live_play_num = filterRdd.map(e=>e._4).count()
          val live_user_num = filterRdd.map(e=>e._4).distinct().count()
          //Filtering the bad data
          val live_duration = filterRdd.map(e=>e._6).filter(e=>{e<14400}).reduce((x,y)=>x+y)
//          val live_aver_duration = live_duration/live_user_num/60

          val insertLivePlaySql = "insert into mtv_live_play_info(date,play_num,user_num,total_duration) values (?,?,?,?)"
          util.insert(insertLivePlaySql,day,new JLong(live_play_num),new JLong(live_user_num),new JLong(live_duration))






          cal.add(Calendar.DAY_OF_MONTH, -1)
          logRdd.unpersist()
          df.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }



  }
}
