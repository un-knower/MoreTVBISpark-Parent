package com.moretv.bi.report.medusa.newsRoomKPI

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.MedusaSubjectNameCodeUtil
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.Map

/**
  * Created by xiajun on 2016/5/16.
  * 统计维度：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
  *
  */
object EachChannelPlayInfo extends BaseClass {
  private val regex ="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(EachChannelPlayInfo, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput)
            .select("userId", "contentType", "launcherAreaFromPath",
              "launcherAccessLocationFromPath", "pageDetailInfoFromPath", "pathIdentificationFromPath", "path",
              "pathPropertyFromPath", "flag", "event")
            .repartition(10)
            .registerTempTable("log_data")

          val df = sqlContext.sql("select userId,contentType,pathIdentificationFromPath,path,pathPropertyFromPath,flag," +
            "event from log_data where event in ('startplay','playview')")

          val rdd = df.map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3), e
            .getString(4), e.getString(5), e.getString(6)))

          val playRdd = rdd.repartition(20)
            .map(e => (getChannelType(e._2, e._3, e._4, e._5, e._6), e._1))
            .repartition(16)
            .cache()

          val playNumRdd = playRdd.map(e => (e._1, 1)).reduceByKey(_ + _)
          val playUserRdd = playRdd.distinct().map(e => (e._1, 1)).reduceByKey(_ + _)
          val mergerRdd = (playNumRdd join playUserRdd).collect

          if (p.deleteOld) {
            val deleteSql = "delete from tmp_medusa_newsroom_kpi_each_channel_play_info where day=?"
            util.delete(deleteSql, insertDate)
          }
          val sqlInsert = "insert into tmp_medusa_newsroom_kpi_each_channel_play_info(day,channel,play_num,play_user) " +
            "values (?,?,?,?)"

          mergerRdd.foreach(e => {
            util.insert(sqlInsert, insertDate, e._1, new JLong(e._2._1), new JLong(e._2._2))
          })

          playRdd.unpersist()
        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

  def getChannelType(contentType: String, subjectInfo: String, path: String, pathSpecial: String, flag: String): String = {
    if (subjectInfo != null) {
      flag match {
        case "medusa" => {
          val subject = MedusaSubjectNameCodeUtil.getSubjectCode(subjectInfo)
          val subjectCode = if (subject == " ") {
            CodeToNameUtils.getSubjectCodeByName(subjectInfo)
          } else {
            subject
          }
          //          val subjectCode = CodeToNameUtils.getSubjectCodeByName(subjectInfo)
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
    } else {
      fromEngToChinese(contentType)
    }
  }

  def fromEngToChinese(str: String): String = {
    str match {
      case "movie" => "电影"
      case "tv" => "电视"
      case "hot" => "资讯短片"
      case "kids" => "少儿"
      case "zongyi" => "综艺"
      case "comic" => "动漫"
      case "jilu" => "纪实"
      case "sports" => "体育"
      case "xiqu" => "戏曲"
      case "mv" => "音乐"
      case _ => "未知"
    }
  }
}
