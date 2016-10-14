package com.moretv.bi.temp.content

import com.moretv.bi.util.{DBOperationUtils, ProgramRedisUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import java.lang.{Long => JLong}
/**
  * Created by witnes on 10/12/16.
  */
object MovieCollectStat extends BaseClass {


  private val tableName = "movie_collect_stat"

  private val dataSource = "play"

  private val fields = "period, contentType, videoSid, videoName, pv, uv"

  private val insert1Sql = s"insert into $tableName ($fields) values(?,?,?,?,?,?)"

  private val delete1Sql = s"delete from $tableName  where day = ?"


  def main(args: Array[String]) {

    ModuleClass.executor(MovieCollectStat, args)
  }

  override def execute(args: Array[String]): Unit = {

    val util = new DBOperationUtils("medusa")

    val loadPath = "/log/medusa/parquet/2016{06*,07*,08*,09*,10*}/play"

    sqlContext.read.parquet(loadPath).registerTempTable("log_data")

    val df = sqlContext.sql("select videoSid, count(userId) as pv, count(distinct userId) as uv  " +
      "from log_data where pathMain like '%collect%' and contentType='movie' " +
      "and date between '2016-06-01' and '2016-10-10' group by videoSid order by pv desc ")

    df.collect.foreach(w => {
      val videoSid = w.getString(0)
      val videoName = ProgramRedisUtil.getTitleBySid(videoSid)
      val pv = w.getLong(1)
      val uv = w.getLong(2)
      util.insert(insert1Sql, "2016-06-01-2016-10-10","movie-collect",videoSid,videoName,
        new JLong(pv), new JLong(uv))

    })


  }
}
