package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.util.Calendar
import java.lang.{Long => JLong}
import java.lang.{Float => JFloat}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.mutable.{ListBuffer}


/**
  * Created by witnes on 9/20/16.
  */

/**
  * 音乐频道指定入口下的各视频播放量统计
  *
  * 数据源: play
  *
  * 提取特征: pathMain , userId , duration
  *
  * 过滤条件: 收藏,推荐,推荐-电台, 榜单, 分类, 搜索,歌手, 舞蹈,精选集, 演唱会
  *
  * 统计: pv ,uv, mean_duration
  *
  * 输出: tbl[mv_tabs_play_statistic](day, home_tab, entrance, pv, uv, mean_duration)
  *
  */

object MVTabPlayStat extends BaseClass{

    private val tableName = "mv_tabs_play_statistic"

    private val insertSql = s"insert into $tableName(day,home_tab,entrance,uv,pv,mean_duration) values(?,?,?,?,?,?)"

    private val deleteSql = s"delete from $tableName where day = ?"


    private val filterTabRegex = (

      "(mv\\*mvRecommendHomePage|mv\\*mvTopHomePage|mv\\*mvCategoryHomePage|mv-search|" +

        "mv\\*mineHomePage\\*site_collect|mv\\*function\\*site_hotsinger|mv\\*function\\*site_dance|" +

        "mv\\*function\\*site_mvsubject|mv\\*function\\*site_concert)(.+-mv_station|" +

        "\\*site_mvstyle-mv_category|\\*site_mvarea-mv_category|\\*site_mvyear-mv_category)*"

    ).r


  def main(args: Array[String]) {

    ModuleClass.executor(MVTabPlayStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) =>{

        // init & util
        val util = new DBOperationUtils("medusa")

        val startDate = p.startDate

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w=>{

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/play"
          println(loadPath)


          val df = sqlContext.read.parquet(loadPath)
                      .select("pathMain","event","userId", "duration")
                      .filter("pathMain is not null")
                      .filter("duration is not null and duration < 10800 and duration >= 0 ")
                      .cache

          //rdd(home_tab, entrance, event, userId, duration)

          val rdd =
                  df.flatMap(
                    e =>
                      path2Tabs(
                        e.getString(0), e.getString(1), e.getString(2), e.getLong(3)
                      )
                  )
                    .filter(_._1 != null)
                    .cache


          //pvuvRdd(home_tab, entrance, userId) with (event = startplay)

          val pvuvRdd =
                        rdd.filter(_._3 == "startplay")
                            .map(e =>((e._1, e._2), e._4))


          //durationRdd(home_tab, entrance, duration) with (event = userexit|selfend)

          val uvRddForDuration = rdd.filter( e => { e._3 == "userexit" || e._3 == "selfend" })
                             .map(e=>((e._1, e._2), e._4))

          val durationRdd =
                            rdd.filter( e => { e._3 == "userexit" || e._3 == "selfend" })
                              .map(e=>((e._1, e._2), e._5))


          //aggregate

          val uvMap = pvuvRdd.distinct.countByKey

          val pvMap = pvuvRdd.countByKey

          val uvMap2 = uvRddForDuration.distinct.countByKey

          val durationMap = durationRdd.reduceByKey(_+_).collectAsMap()


          //deal with table

          if(p.deleteOld){
            util.delete(deleteSql,sqlDate)
          }

          uvMap.foreach(w => {

            val key = w._1

            val uv2 = uvMap2.get(key) match {

              case Some(p) => p
              case None => 0

            }

            val meanDuration = durationMap.get(key) match {

              case Some(p) => p.toFloat / uv2
              case None => 0

            }

            val pv = pvMap.get(w._1) match {

              case Some(p) => p
              case None => 0

            }

            util.insert(
              insertSql, sqlDate, w._1._1, w._1._2, new JLong(w._2), new JLong(pv), new JFloat(meanDuration)
            )


          })

        })
      }
      case None => {

        throw new Exception("MVPlayVideoRankStat fails")

      }
    }
  }

  /**
    *
    * @param path pathMain 用于确定视频入口
    * @return  entrance 视频入口
    */
  def path2Tabs( path:String, event:String, userId:String, duration:Long): List[(String,String,String,String,Long)] = {

    val buf = ListBuffer.empty[(String, String, String, String, Long)]

    val station = ".+-mv_station".r

    filterTabRegex findFirstMatchIn path match {

      case Some(p) => {

        p.group(1) match {

          case "mv*mvRecommendHomePage" => {

            buf.+= (("推荐", "推荐", event, userId, duration))

            p.group(2) match {

              case station =>  buf.+= (("电台", "电台", event, userId, duration))

              case _ => null

            }
          }

          case "mv*mvTopHomePage" => {

            buf.+=(("榜单", "榜单", event, userId, duration))

          }

          case "mv*mvCategoryHomePage" => {

            p.group(2) match {

              case "*site_mvstyle-mv_category" => {

                buf.+= (("分类", "分类", event,  userId, duration))

                buf.+= (("分类", "风格", event,  userId, duration))

              }

              case "*site_mvarea-mv_category" => {

                buf.+= (("分类", "分类", event, userId, duration))

                buf.+= (("分类", "地区", event, userId, duration))

              }

              case "*site_mvyear-mv_category" => {

                buf.+= (("分类", "分类", event,  userId, duration))

                buf.+= (("分类", "年代", event,  userId, duration))

              }

              case _ => null
            }

          }

          case "mv-search" => {

            buf.+= (("入口", "音乐搜索", event, userId, duration))

          }

          case "mv*mineHomePage*site_collect" => {

            buf.+= (("我的", "音乐收藏", event, userId, duration))

          }

          case "mv*function*site_hotsinger" => {

            buf.+= (("入口","歌手", event, userId, duration))

          }

          case "mv*function*site_dance" => {

            buf.+= (("入口", "舞蹈", event, userId, duration))

          }

          case "mv*function*site_mvsubject" => {

            buf.+= (("入口", "精选集", event, userId, duration))

          }

          case "mv*function*site_concert" => {

            buf.+= (("入口", "演唱会", event, userId, duration))

          }

          case _ => null
        }

      }

      case None => null
    }

    buf.toList

  }


}
