package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/5/16.
 *
 */
object kidstop200201601to06 extends SparkSetting{
  def main(args: Array[String]) {
        config.set("spark.executor.memory", "5g").
          set("spark.executor.cores", "5").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        sqlContext.read.parquet("/log/medusa/parquet/20160{1*,2*,3*,4*,5*,6*}/play").select("userId","event","videoSid",
          "contentType")
          .registerTempTable("log1")
        sqlContext.read.parquet("/mbi/parquet/playview/20160{1*,2*,3*,4*,5*,6*}").select("userId","videoSid","contentType")
          .registerTempTable("log2")

    val medusaDF = sqlContext.sql("select videoSid,userId from log1 where event = 'startplay' and contentType='kids'")
    val moretvDF = sqlContext.sql("select videoSid,userId from log2 where contentType = 'kids'")

    val merger = medusaDF.unionAll(moretvDF).registerTempTable("log")
    val top200 = sqlContext.sql("select videoSid,count(userId) as num from log group by videoSid order by num desc limit " +
      "200").map(e=>(e.getString(0),e.getLong(1)))

    val insertSql = "insert into temp_kids_top200(sid,title,playnum) values (?,?,?)"
    top200.collect().foreach(e=>{
      util.insert(insertSql,e._1,ProgramRedisUtil.getTitleBySid(e._1),new JLong(e._2))
    })


  }

}
