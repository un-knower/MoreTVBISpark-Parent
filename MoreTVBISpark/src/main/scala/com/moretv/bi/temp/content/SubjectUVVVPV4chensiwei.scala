package com.moretv.bi.temp.content

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by liankai on 2016/04/11.
  * 计算某个视频在某段时间内的播放人数和播放次数
 */
object SubjectUVVVPV4chensiwei extends SparkSetting with DateUtil{
  def main(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.cores.max", "200")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        //calculate log whose type is play
        var path = "/mbi/parquet/playview/2016????"
        sqlContext.read.load(path).filter("path like '%zongyi131%'").registerTempTable("log_data")
        sqlContext.sql("select count(distinct userId),count(userId) from log_data").collect().
          foreach(row => println(row.getAs[Long](0)+","+row.getLong(1)))

        path = "/mbi/parquet/detail/2016????"
        sqlContext.read.load(path).filter("path like '%zongyi131%'").registerTempTable("detail_data")
        sqlContext.sql("select count(distinct userId),count(userId) from detail_data").collect().
          foreach(row => println(row.getAs[Long](0)+","+row.getLong(1)))

      }
      case None =>{
        throw new RuntimeException("At least need param --startDate.")
      }
    }

  }

}
