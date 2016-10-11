package com.moretv.bi.temp.content

import java.lang.{Long => JLong}

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Created by xiajun and zhangyu on 2016/8/3.
 * 统计电视猫每月分频道播放的人数次数
 * 统计维度：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
 *
 */
object MonthUVStatisticsBasedContentType extends SparkSetting{
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r

  def main(args: Array[String]) {

        config.set("spark.executor.memory", "5g").
          set("spark.executor.cores", "10").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")

      import sqlContext.implicits._


    val medusaDir = "/log/medusaAndMoretvMerger/"
        val playviewInput = s"$medusaDir/20160{802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,901}/playview"

        sqlContext.read.parquet(playviewInput).select("userId","contentType","launcherAreaFromPath",
            "launcherAccessLocationFromPath","pageDetailInfoFromPath","pathIdentificationFromPath","path",
            "pathPropertyFromPath","flag","event").registerTempTable("log_data")

        val df = sqlContext.sql("select userId,contentType,pathIdentificationFromPath,path,pathPropertyFromPath,flag," +
            "event from log_data where event in ('startplay','playview')").repartition(20)
        val rdd = df.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e
          .getString(4),e.getString(5),e.getString(6)))

        val playRdd = rdd.map(e=>(getChannelType(e._2,e._3,e._4,e._5,e._6),e._1)).toDF("channel","userId").registerTempTable("log")


    sqlContext.sql("select channel as channel,count(distinct userId) as user_num, count(userId) as access_num from log group by channel").show(20,false)

  }

  def getChannelType(contentType:String,subjectInfo:String,path:String,pathSpecial:String,flag:String):String= {
    if (subjectInfo != null) {
      flag match {
        case "medusa" => {
          val subjectCode = CodeToNameUtils.getSubjectCodeByName(subjectInfo)
          regex findFirstMatchIn subjectCode match {
            case Some(m) => fromEngToChinese(m.group(1))
            case None => fromEngToChinese(contentType)
          }
        }
        case "moretv" => {
          regex findFirstMatchIn subjectInfo match {
            case Some(m) => fromEngToChinese(m.group(1))
            case None => fromEngToChinese(contentType)
          }
        }
        case _ => fromEngToChinese(" ")
      }
    }else{
      fromEngToChinese(contentType)
    }
  }

  def fromEngToChinese(str:String):String={
    str match {
      case "movie"=>"电影"
      case "tv"=>"电视"
      case "hot"=>"资讯短片"
      case "kids"=>"少儿"
      case "zongyi"=>"综艺"
      case "comic"=>"动漫"
      case "jilu"=>"纪实"
      case "sports"=>"体育"
      case "xiqu"=>"戏曲"
      case "mv"=>"音乐"
      case _ => "未知"
    }
  }
}
