package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util
import java.util.Calendar

import cn.whaley.bi.utils.ElasticSearchUtil
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.elasticsearch.common.xcontent.XContentFactory._

/**
 * Created by xiajun on 2016/5/16.
 *
 */
object EachVideoPlayInfo2ES extends BaseClass{
  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(EachVideoPlayInfo2ES,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput).select("userId","contentType","event","videoSid")
            .registerTempTable("log_data")

          val rdd = sqlContext.sql("select contentType,videoSid,count(userId),count(distinct userId)" +
            " from log_data where event in ('startplay','playview') group by contentType,videoSid").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e
            .getLong(3))).filter(_!=null).filter(_._2!=null).filter(_._1!=null).filter(_._2.length<=50).collect()

          val resList = new util.ArrayList[util.Map[String,Object]]()

          // 将数据插入ES
          rdd.foreach(e=>{
            val resMap = new util.HashMap[String,Object]()
            resMap.put("contentType",e._1.toString)
            resMap.put("sid",e._2.toString)
            resMap.put("title",ProgramRedisUtil.getTitleBySid(e._2).toString)
            resMap.put("day",insertDate.toString)
            resMap.put("userNum",new JLong(e._4))
            resMap.put("accessNum",new JLong(e._3))
            resList.add(resMap)
          })
          ElasticSearchUtil.bulkCreateIndex(resList,"medusa","programPlay")

        })
        ElasticSearchUtil.close
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
