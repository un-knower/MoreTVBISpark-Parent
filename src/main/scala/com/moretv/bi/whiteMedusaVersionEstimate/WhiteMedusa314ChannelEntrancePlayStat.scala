package com.moretv.bi.whiteMedusaVersionEstimate

import java.lang.{Float => JFloat, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by zhu.bingxin on 2017/5/4.
  * 统计日志：电视猫3X日志，白猫3.1.4版本
  * 统计维度：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
  * 统计指标：播放次数、播放人数
  */
object WhiteMedusa314ChannelEntrancePlayStat extends BaseClass {

  private val tableName = "white_medusa_314_channel_entrance_Play_st"

  private val fields = "day,contentType,entrance,playNum,playUser,playDuration"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ? "

  private val regex ="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r

  private val sourceRe = ("(home\\*classification|search|home\\*my_tv\\*history|" +
    "home\\*my_tv\\*collect|home\\*recommendation|home\\*my_tv\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,})").r

  private val sourceRe1 = ("(classification|history|hot|recommend|search)").r

  private val codeMap: scala.collection.immutable.Map[String, String] = CodeToNameUtils.getSubjectCodeMap


  def main(args: Array[String]) {

    ModuleClass.executor(WhiteMedusa314ChannelEntrancePlayStat, args)

  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sqlContext = new SQLContext(sc)

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {


          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //TODO 是否需要写到固定的常量类or通过SDK读取
          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MERGER, LogTypes.PLAYVIEW)
            .filter("flag = 'medusa' and path is not null or pathMain is not null") //只取3X数据
            .filter("apkVersion = '3.1.4'") //筛选出3.1.4版本
            .select(
            "event", "userId", "pathMain", "path", "contentType", "pathIdentificationFromPath", "flag", "duration"
          )
            .registerTempTable("log_data")


          val dfUser: DataFrame = sqlContext.sql(
            """
              |select userId,pathMain,path,contentType,pathIdentificationFromPath,flag,cast(0 as Long)
              |  from log_data
              |  where event in ('startplay','playview')
            """.stripMargin
          )

          val dfDuration: DataFrame = sqlContext.sql(
            """
              |select userId,pathMain,path,contentType,pathIdentificationFromPath,flag,duration
              | from log_data
              | where event not in ('startplay')
              |   and duration between 1 and 10800
            """.stripMargin
          )

          //(channel,入口),userId
          val userRdd = contentFilter(dfUser)
            .map(e => ((e._1, e._2), e._4))

          val durationRdd = contentFilter(dfDuration)
            .map(e => ((e._1, e._2), e._3))

          val uvMap = userRdd.distinct.countByKey

          val pvMap = userRdd.countByKey

          val durationMap = durationRdd.reduceByKey(_ + _).collectAsMap


          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          uvMap.foreach(w => {

            val key = w._1
            val contentType = fromEngToChinese(w._1._1)
            //val channel = w._1._1
            val entrance = w._1._2
            val playUser = new JLong(w._2)
            val playNum = new JLong(pvMap.get(key) match {
              case Some(p) => p
              case None => 0
            })
            val playDuration = new JFloat(durationMap.get(key) match {
              case Some(p) => p.toFloat / playUser
              case None => 0
            })
            //println(channel, uv, pv)
            util.insert(insertSql, sqlDate, contentType, entrance, playNum, playUser, playDuration)
          })

        })

      }

      case None => {

      }
    }

  }

  //dfUser: userId,pathMain,path,contentType,pathIdentificationFromPath,flag,cast(0 as Long)
  //dfDuration : userId,pathMain,path,contentType,pathIdentificationFromPath,flag,duration
  def contentFilter(df: DataFrame): RDD[(String, String, Long, String)] = {

    val rdd = df.map(e => {
      var channel = e.getString(3)
      if (e.getString(4) != null) {
        channel = regex findFirstMatchIn e.getString(4) match {
          case Some(p) => p.group(1)
          case None => {
            regex findFirstMatchIn codeMap.getOrElse(e.getString(4), e.getString(3)) match {
              case Some(pp) => pp.group(1)
              case None => e.getString(3)
            }
          }
        }
      }
      //                    pathMain        path             flag              0             userId
      (channel, splitSource(e.getString(1), e.getString(2), e.getString(5)), e.getLong(6), e.getString(0))

    })
      .filter(
        e => (e._1 == "movie" || e._1 == "kids" || e._1 == "tv" || e._1 == "sports" || e._1 == "kids"
          || e._1 == "reservation" || e._1 == "mv" || e._1 == "jilu" || e._1 == "comic" || e._1 == "zongyi"
          || e._1 == "hot" || e._1 == "xiqu"
          ))

      .filter(_._2 != null)
    rdd
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

  //pathMain        path             flag
  def splitSource(pathMain: String, path: String, flag: String): String = {
    val specialPattern = "home\\*my_tv\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,}".r
    flag match {
      case "medusa" => {
        sourceRe findFirstMatchIn pathMain match {
          case Some(p) => {
            p.group(1) match {
              case "home*classification" => "分类入口"
              case "home*my_tv*history" => "历史"
              case "home*my_tv*collect" => "收藏"
              case "home*recommendation" => "首页推荐"
              case "search" => "搜索"
              case _ => {
                if (specialPattern.pattern.matcher(p.group(1)).matches) {
                  "自定义入口"
                }
                else {
                  "其它3"
                }
              }
            }
          }
          case None => "其它3"
        }
      }
      case "moretv" => {
        sourceRe1 findFirstMatchIn path match {
          case Some(p) => {
            p.group(1) match {
              case "classification" => "分类入口"
              case "history" => "历史"
              case "hotrecommend" => "首页推荐"
              case "search" => "搜索"
              case _ => "其它2"
            }
          }
          case None => "其它2"
        }
      }
    }

  }

}