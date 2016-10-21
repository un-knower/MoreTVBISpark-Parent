package com.moretv.bi.temp

import com.moretv.bi.util.{DBOperationUtils, FileUtils}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import java.lang.{Long => JLong}

import org.apache.spark.storage.StorageLevel

/**
  * Created by witnes on 10/13/16.
  */
object BigBang extends BaseClass {

  private val tableName = "gradation_users_stat"

  private val fields = "day,event,pv,uv,duration"

  private val insertSql = s"insert into $tableName(day,event,pv,uv,duration)(?,?,?,?,?)"

  private val inserSql = s"delete from $tableName where day = ?"

  private val playEvent = "'startplay'"

  private val playDurationEvent = "'selfend','userexit'"

  def main(args: Array[String]) {
    ModuleClass.executor(BigBang, args)
  }

  override def execute(args: Array[String]): Unit = {

    val util = new DBOperationUtils("medusa")

    val dataUserPath = "/log/medusa/temple/gradationUsers"

    sqlContext.read.parquet(dataUserPath).registerTempTable("log")

    val loadAllPath = "/log/medusa/parquet/2016{09*,10*}/*"

    val loadPlayPath = "/log/meudsa/parquet/2016{09*,10*}/play"

    sqlContext.read.parquet(loadAllPath).registerTempTable("log_all")

    sqlContext.read.parquet(loadPlayPath).registerTempTable("log_play")


    val gradationUserDF = sqlContext.sql("select userId from log")

    val dailyDF = sqlContext.sql("select userId from log")

    val playDailyDF = sqlContext.sql(s"select  userId from log_play where event = ${playEvent}")

    val playDurationDF = sqlContext.sql(s"select userId, duration from log_play where event in ${playDurationEvent} " +
      s"and duration betwwen 0 and 10800")


    // 日活
    val dau = (gradationUserDF.intersect(dailyDF)).count()

    // 播放
    val playDF = (gradationUserDF.intersect(playDailyDF)).persist(StorageLevel.MEMORY_AND_DISK)
    val playUV = playDF.distinct().count()
    val playPV = playDF.count()
    val gradationArr = gradationUserDF.map(e=>e.getString(0)).collect()
    val playDurationRDD = playDurationDF.map(e=>(e.getString(0),e.getLong(1))).filter(e=>gradationArr.contains(e._1)).
      map(e=>e._2)






    // for all

    val allDf = sqlContext.sql(
      "select date, count(userId) as pv , count(distinct userId) as uv from log_data_all " +
        "group by date")

    // for play and  duration
//    df2.filter(s"event in ($playDurationEvent)").registerTempTable("log_data_duration")
//
//    val playDurationDf = sqlContext.sql(
//
//      "select tbl1.date, count(tbl1.userId) as pv, count(distinct tbl1.userId) as uv, sum(tbl2.duration)/count(distinct tbl1.userId) " +
//
//        "from log_data_play as tbl1 join log_data_duration as tbl2" +
//
//        " on tbl1.userId=tbl2.userId " +
//
//        "group by tbl1.date"
//    )

//    playDurationDf.collect.foreach(w => {
//      util.insert(inserSql, w.getString(0), "播放", new JLong(w.getLong(1)), new JLong(w.getLong(2)), new JLong(w.getLong(3)))
//    })
//
//    allDf.collect.foreach(w => {
//      util.insert(inserSql, w.getString(0), "活跃", new JLong(w.getLong(1)), new JLong(w.getLong(2)), new JLong(0))
//    })


  }

}
