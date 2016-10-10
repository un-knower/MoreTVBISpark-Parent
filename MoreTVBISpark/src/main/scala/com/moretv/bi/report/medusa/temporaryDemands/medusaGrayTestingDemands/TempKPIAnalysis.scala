package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by xiajun on 2016/5/16.
 * 统计维度：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
 *
 */
object TempKPIAnalysis extends SparkSetting{
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "5g").
          set("spark.executor.cores", "5").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput).select("userId","contentType","launcherAreaFromPath",
            "launcherAccessLocationFromPath","pageDetailInfoFromPath","pathIdentificationFromPath","path",
            "pathPropertyFromPath","flag","event").registerTempTable("log_data")

          val df = sqlContext.sql("select userId,contentType,pathIdentificationFromPath,path,pathPropertyFromPath,flag," +
            "event from log_data where event in ('startplay','playview') and contentType in ('comic','sports','sport')")
            .repartition(20)
          val rdd = df.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e
            .getString(4),e.getString(5),e.getString(6)))

          val playRdd = rdd.map(e=>((getChannelType(e._2,e._3,e._4,e._5,e._6),e._2),e._1)).distinct().countByKey()

          if(p.deleteOld){
            val deleteSql="delete from temp_medusa_kpi_analysis where day=?"
            util.delete(deleteSql,insertDate)
          }
          val sqlInsert = "insert into temp_medusa_kpi_analysis(day,channel,content_type,user_id) " +
            "values (?,?,?,?)"

          playRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1._1,e._1._2,new JLong(e._2))
          })
        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
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
