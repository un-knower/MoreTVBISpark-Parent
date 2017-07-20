package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.util.MedusaSubjectNameCodeUtil
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by xiajun on 2016/5/16.
  * 统计不同专题的播放量,用于展示各个频道的专题排行榜数据
  */
object EachSubjectPlayInfo extends BaseClass {
  private val regex ="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv|interest)([0-9]+)""".r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).
            select("userId", "launcherAreaFromPath", "launcherAccessLocationFromPath",
            "pageDetailInfoFromPath", "pathIdentificationFromPath", "path", "pathPropertyFromPath", "flag", "event")
            .registerTempTable("log_data")

          val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
            "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from log_data where event in ('startplay'," +
            "'playview')").repartition(20)
          val formattedRdd = rdd.map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3), e
            .getString(4), e.getString(5), e.getString(6), e.getString(7))).persist(StorageLevel.MEMORY_AND_DISK)

          val medusaInfoRdd = formattedRdd.filter(_._8 == "medusa").filter(_._7 == "subject").map(e => {
            val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(e._5)
            if (subjectCode != " ") {
              (getSubjectTypeFromSubjectCode(subjectCode, "moretv"), subjectCode, e._1)
            } else {
              (getSubjectTypeFromSubjectCode(e._5, e._8), getSubjectCode(e._5, e._8), e._1)
            }
          })

          val moretvInfoRdd = formattedRdd.filter(_._8 == "moretv").map(e => (e._1, e._6)).flatMap(e =>
            SubjectUtils.getSubjectCodeAndPathWithId(e._2, e._1)).map(e => (e._1._1, e._2)).filter(_._1 != null).map(e =>
            (getSubjectTypeFromSubjectCode(e._1, "moretv"), e._1, e._2))

          val mergerInfoRdd = medusaInfoRdd union moretvInfoRdd

          val typeInfoRdd = mergerInfoRdd.filter(_._1 != null).persist(StorageLevel.MEMORY_AND_DISK)
//          val eachSubjectPlayNumMap = typeInfoRdd.map(x => ((x._1, x._2), 1l)).reduceByKey(_ + _).collectAsMap()
//          val eachSubjectPlayUserMap = typeInfoRdd.distinct().map(x => ((x._1, x._2), 1l)).reduceByKey(_ + _).collectAsMap()
          val eachSubjectPlayNumMap = typeInfoRdd.map(x => ((x._1, x._2), 1l)).reduceByKey(_ + _)
          val eachSubjectPlayUserMap = typeInfoRdd.distinct().map(x => ((x._1, x._2), 1l)).reduceByKey(_ + _)
          val mergerRdd = eachSubjectPlayNumMap.join(eachSubjectPlayUserMap).map(e=>(e._1._1,e._1._2,e._2._1,e._2._2))
//          val mergerRdd = eachSubjectPlayNumMap.map(e => (e._1._1, e._1._2, e._2, eachSubjectPlayUserMap(e._1)))

          val sqlContextTmp = sqlContext
          import sqlContextTmp.implicits._
          mergerRdd.toDF("channel", "subject_code", "play_num", "play_user")
            .registerTempTable("result_data")

          val mergerRdd_top200 = sqlContext.sql(
            """
              |(SELECT * from result_data where channel = '动漫' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '资讯短片' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '纪实' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '少儿' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '电影' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '奇趣' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '电视' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '戏曲' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '体育' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '音乐' ORDER BY play_num DESC LIMIT 200) union all
              |(SELECT * from result_data where channel = '综艺' ORDER BY play_num DESC LIMIT 200)
            """.stripMargin)
            .map(e => (e.getString(0), e.getString(1), e.getLong(2), e.getLong(3))) //分频道的top200数据

          if (p.deleteOld) {
            val deleteSql = "delete from medusa_each_subject_play_info where day=?"
            util.delete(deleteSql, insertDate)
          }
          val sqlInsert = "insert into medusa_each_subject_play_info(day,channel,subject_code,subject_title,play_num," +
            "play_user) values (?,?,?,?,?,?)"
          val insertSqlTop = "insert into medusa_each_subject_play_info_top(day,channel,subject_code,subject_title," +
            "play_num,play_user) values (?,?,?,?,?,?)"

          mergerRdd.foreachPartition(partition=> {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(e => {
              util1.insert(sqlInsert, insertDate, e._1, e._2, CodeToNameUtils.getSubjectNameBySid(e._2), new JLong(e._3), new JLong(e._4))
            })
          })

          mergerRdd_top200.foreachPartition(partition => {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(e => {
              util1.insert(insertSqlTop, insertDate, e._1, e._2, CodeToNameUtils.getSubjectNameBySid(e._2), new JLong(e._3), new JLong(e._4))
            })
          })

          formattedRdd.unpersist()
          typeInfoRdd.unpersist()
        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

  def getSubjectTypeFromSubjectCode(subjectInfo: String, flag: String): String = {
    if (subjectInfo != null) {
      if (flag == "moretv") {
        regex findFirstMatchIn subjectInfo match {
          case Some(m) => fromEngToChinese(m.group(1))
          case None => null
        }
      } else if (flag == "medusa") {
        val subjectCode = CodeToNameUtils.getSubjectCodeByName(subjectInfo)
        regex findFirstMatchIn subjectCode match {
          case Some(m) => fromEngToChinese(m.group(1))
          case None => null
        }
      } else null
    } else null
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
      case "interest" => "奇趣"
    }
  }

  def getSubjectCode(subjectInfo: String, flag: String): String = {
    if (subjectInfo != null) {
      if (flag == "moretv") {
        regex findFirstMatchIn subjectInfo match {
          case Some(p) => p.group(1) + p.group(2)
          case None => null
        }
      } else if (flag == "medusa") {
        val subjectCode = CodeToNameUtils.getSubjectCodeByName(subjectInfo)
        regex findFirstMatchIn subjectCode match {
          case Some(p) => p.group(1) + p.group(2)
          case None => null
        }
      } else null
    } else null
  }
}
