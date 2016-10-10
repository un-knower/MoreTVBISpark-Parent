package com.moretv.bi.temp.content

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by liankai on 2016/04/11.
  * 计算某个视频在某段时间内的播放人数和播放次数
 */
object ProgramUVPV4chensiwei extends SparkSetting with DateUtil{
  def main(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.cores.max", "200")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        //calculate log whose type is play
        val path = "/mbi/parquet/detail/2016????"
        val sid = p.sid
        sqlContext.read.load(path).filter(s"videoSid = '$sid'").registerTempTable("log_data")
        val result = sqlContext.sql("select count(distinct userId),count(userId) from log_data").collect()
        result.foreach(row => println(row.getAs[Long](0)+","+row.getLong(1)))

      }
      case None =>{
        throw new RuntimeException("At least need param --startDate.")
      }
    }

  }

}
