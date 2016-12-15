package com.moretv.bi.report.medusa.tempAnalysis

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/7/27.
 * 统计奥运各个tab中各个视频的播放人数、次数
 *
 */
object OlympicAreaDistributionAnalysis extends SparkSetting{

  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext.udf.register("IPLocation", IPLocationDataUtil.getProvince _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusa/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        val date = DateFormatUtils.readFormat.format(calendar.getTime)
        calendar.add(Calendar.DAY_OF_MONTH,-1)

        val playviewInput = s"$medusaDir/201608*/play/"

        sqlContext.read.parquet(playviewInput).select("userId","videoSid","event","pathMain","videoName","ip")
          .registerTempTable("log_data")

        /**
         * 统计各个省份的播放情况
         */
        val playInfoByProvinceDF = sqlContext.sql("select IPLocation(ip),count(userId),count" +
          "(distinct userId) from log_data where event ='startplay' and pathMain like '%olympic%' group by IPLocation(ip)").
          map(e=>(e.getString(0),e.getLong(1),e.getLong(2)))
        val insertSQL1 = "insert into olympic_province_temp(province,num,user) values (?,?,?)"
        playInfoByProvinceDF.collect().foreach(i=>{
          util.insert(insertSQL1,i._1,new JLong(i._2),new JLong(i._3))
        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

  def getOlympicTabName(path:String)={
    var result:String=null
    try{
      if(path.contains("home*classification")){
        result = path.split("-")(2).split("\\*")(1)
      } else if(path.contains("home*recommendation")) {
        result = path.split("-")(1).split("\\*")(1)
      }
    } catch {
      case e:Exception => e.printStackTrace()
    }
    result
  }

}
