package com.moretv.bi.report.medusa.newsRoomKPI

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.MedusaSubjectNameCodeUtil
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xiajun on 2016/5/16.
  * 统计维度：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
  *
  */
object HotChannelPlayInfo extends BaseClass {

  private val tableName = "hot_source_play_stat"

  private val fields = "day,entrance,pv,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName($fields) where day = ?"

  private val regex ="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r

  private val sourceRe = ("(home\\*classification\\*hot|" +
    "search|home\\*my_tv*history|home\\*my_tv*collect|home\\*recommendation|home\\*my_tv\\*hot|)").r

  private val sourceRe1 = ("(classification|history|hotrecommend)").r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "80")
    ModuleClass.executor(HotChannelPlayInfo, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")

        sqlContext.udf.register("splitSource", splitSource _)
        sqlContext.udf.register("getChannelType", getChannelType _)

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          val playviewInput = s"/log/medusaAndMoretvMerger/$loadDate/playview"

          sqlContext.read.parquet(playviewInput)

            .filter("pathMain is not null or path is not null")

            .filter("event in ('startplay','playview')")

            .registerTempTable("log_data")


          sqlContext.sql("select userId,pathMain, path, flag, " +

            "getChannelType(contentType,pathIdentificationFromPath,flag) as contenttype " +

            "from log_data where getChannelType(contentType,pathIdentificationFromPath,flag) = '资讯短片' ")

            .registerTempTable("log_data_1")


          val df = sqlContext.sql(
            "select splitSource(pathMain,path,flag) , count(userId) as pv , count(distinct userId) as uv " +

              "from log_data_1 group by splitSource(pathMain,path,flag)"
          )

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          df.show(30)

          df.collect.foreach(w => {

            println(w)
            val source = w.getString(0)

            val uv = new JLong(w.getLong(1))

            val pv = new JLong(w.getLong(2))

            util.insert(insertSql, sqlDate, source, pv, uv)
          })


        })

      }

      case None => {

      }
    }
  }


  def getChannelType(contentType: String, subjectInfo: String, flag: String): String = {

    if (subjectInfo != null) {

      flag match {
        case "medusa" => {

          val subject = MedusaSubjectNameCodeUtil.getSubjectCode(subjectInfo)

          val subjectCode = if (subject == " ") {
            CodeToNameUtils.getSubjectCodeByName(subjectInfo)
          } else {
            subject
          }

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

  def splitSource(pathMain: String, path: String, flag: String): String = {

    flag match {
      case "medusa" => {
        sourceRe findFirstMatchIn pathMain match {
          case Some(p) => {
            p.group(1) match {
              case "home*classification*hot" => "分类入口"
              case "home*my_tv*history" | "" => "历史"
              case "home*my_tv*collect" | "" => "收藏"
              case "home*my_tv*hot" | "" => "自定义入口"
              case "home*recommendation" | "" => "首页推荐"
              case _ => null
            }
          }
          case None => null
        }
      }
      case "moretv" => {
        sourceRe1 findFirstMatchIn path match {
          case Some(p) => {
            p.group(1) match {
              case "classification" => "分类入口"
              case "history" | "" => "历史"
              case "hotrecommend" | "" => "首页推荐"
              case _ => null
            }
          }
          case None => null
        }
      }
    }

  }

}
