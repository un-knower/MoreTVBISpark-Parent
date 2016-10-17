package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Float => JFloat, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

import scala.collection.mutable.ListBuffer


/**
  * Created by witnes on 9/20/16.
  */

/**
  * 领域: mv
  * 对象: 列表
  * 数据源: play
  * 维度: 天, 入口(多级), 视频
  * 提取特征: pathMain , userId , duration
  * 过滤条件: 收藏,推荐,电台, 榜单, 分类, 搜索,歌手, 舞蹈,精选集, 演唱会
  * 统计: pv ,uv, mean_duration
  * 输出: tbl[mv_tab_play_stat](day, tabname, entrance, pv, uv, mean_duration)
  *
  */

object MVTabPlayStat extends BaseClass {

  import com.moretv.bi.report.medusa.channeAndPrograma.mv.{TabPathConst => t}

  private val dataSource = "play"

  private val tableName = "mv_tab_play_stat"

  private val fields = "day,tabname,entrance,uv,pv,mean_duration"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  // 我的，电台，推荐，榜单， 分类, 入口-搜索，入口-歌手，入口-舞蹈，入口-精选集，入口-演唱会
  private val firstPath = t.Mv_First_Path.mkString("|")

  // 分类-风格，分类-地区，分类-年代，榜单-新歌榜，榜单-美国公告榜，榜单-热歌榜，榜单-更多榜单
  private val secondPath = t.Mv_Second_Path.mkString("|")

  // 二级分类 和一级分类下的tab页面 和 视频集
  private val thirdPath = t.Mv_Third_Path.mkString("|")

  private val filterTabRegex = (

    s"($firstPath)($secondPath)?($thirdPath)?"

    ).r


  def main(args: Array[String]) {

    ModuleClass.executor(MVTabPlayStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        // init & util
        val util = new DBOperationUtils("medusa")

        val startDate = p.startDate

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/$dataSource"
          println(loadPath)


          val df = sqlContext.read.parquet(loadPath)
            .select("pathMain", "event", "userId", "duration")
            .filter("pathMain is not null")
            .filter("duration is not null and duration < 10800 and duration >= 0 ")
            .cache

          //rdd(home_tab, entrance, event, userId, duration)
          df.show(50, false)

          val rdd =
            df.flatMap(
              e =>
                path2Tabs(
                  e.getString(0), e.getString(1), e.getString(2), e.getLong(3)
                )
            )
              .filter(_._1 != null)
              .filter(_._2 != null)
              .cache

          println(filterTabRegex)
          rdd.take(20).foreach(println)

          //pvuvRdd(home_tab, entrance, userId) with (event = startplay)

          val pvuvRdd =
            rdd.filter(_._3 == "startplay")
              .map(e => ((e._1, e._2), e._4))


          //durationRdd(home_tab, entrance, duration) with (event = userexit|selfend)

          val uvRddForDuration = rdd.filter(e => {
            e._3 == "userexit" || e._3 == "selfend"
          })
            .map(e => ((e._1, e._2), e._4))

          val durationRdd =
            rdd.filter(e => {
              e._3 == "userexit" || e._3 == "selfend"
            })
              .map(e => ((e._1, e._2), e._5))


          //aggregate

          val uvMap = pvuvRdd.distinct.countByKey

          val pvMap = pvuvRdd.countByKey

          val uvMap2 = uvRddForDuration.distinct.countByKey

          val durationMap = durationRdd.reduceByKey(_ + _).collectAsMap()


          // deal with table

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
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

        throw new Exception("MVTabPlayStat fails")

      }
    }
  }

  /**
    *
    * @param path pathMain 用于确定视频入口
    * @return entrance 视频入口
    */
  def path2Tabs(path: String, event: String, userId: String, duration: Long): List[(String, String, String, String, Long)] = {
    val buf = ListBuffer.empty[(String, String, String, String, Long)]

    import com.moretv.bi.report.medusa.channeAndPrograma.mv.{TabPathConst => t}

    filterTabRegex findFirstMatchIn path match {

      case Some(p) => {

        p.group(1) match {

          case t.Mv_Collect => buf.+=(("我的", "收藏", event, userId, duration))

          case t.Mv_Recommender => buf.+=(("推荐", "推荐", event, userId, duration))

          case t.Mv_Station => buf.+=(("电台", "电台", event, userId, duration))

          case t.Mv_TopList => {

            p.group(2) match {

              case t.Mv_Latest => {
                buf.+=(("榜单", "榜单", event, userId, duration))
                buf.+=(("榜单", "新歌声", event, userId, duration))
              }

              case t.Mv_Billboard => {
                buf.+=(("榜单", "榜单", event, userId, duration))
                buf.+=(("榜单", "美国公告榜", event, userId, duration))
              }

              case t.Mv_Hot => {
                buf.+=(("榜单", "榜单", event, userId, duration))
                buf.+=(("榜单", "热歌榜", event, userId, duration))
              }

              case t.Mv_More => {
                p.group(3) match {
                  case t.Mv_poster => {
                    buf.+=(("榜单", "榜单", event, userId, duration))
                    buf.+=(("榜单", "更多榜单", event, userId, duration))
                  }
                  case _ => null
                }
              }
            }
          }
          case t.Mv_Category => {
            p.group(2) match {

              case t.Mv_Style => {
                p.group(3) match {
                  case t.Mv_Sub_Category => {
                    buf.+=(("分类", "分类", event, userId, duration))
                    buf.+=(("分类", "风格", event, userId, duration))
                    buf.+=(("分类", p.group(3), event, userId, duration))
                  }
                }
              }

              case t.Mv_Year => {
                p.group(3) match {
                  case t.Mv_Sub_Category => {
                    buf.+=(("分类", "分类", event, userId, duration))
                    buf.+=(("分类", "年代", event, userId, duration))
                    buf.+=(("分类", p.group(3), event, userId, duration))
                  }
                }
              }

              case t.Mv_Area => {
                p.group(3) match {
                  case t.Mv_Sub_Category => {
                    buf.+=(("分类", "分类", event, userId, duration))
                    buf.+=(("分类", "地区", event, userId, duration))
                    buf.+=(("分类", p.group(3), event, userId, duration))
                  }
                }
              }

            }
          }
          case t.Mv_Search => {
            buf.+=(("入口", "音乐搜索", event, userId, duration))
          }
          case t.Mv_Singer => {
            buf.+=(("入口", "歌手", event, userId, duration))
          }
          case t.Mv_Dance => {
            p.group(3) match {
              case t.Mv_Sub_Category => {
                buf.+=(("入口", "舞蹈", event, userId, duration))
                buf.+=(("舞蹈", p.group(3), event, userId, duration))
              }
            }
          }
          case t.Mv_Subject => {
            p.group(3) match {
              case t.Mv_poster => {
                buf.+=(("入口", "精选集", event, userId, duration))
              }
            }

          }
          case t.Mv_Concert => {
            p.group(3) match {
              case t.Mv_Sub_Category => {

                buf.+=(("入口", "演唱会", event, userId, duration))
                buf.+=(("演唱会", p.group(3), event, userId, duration))

              }
            }
          }
          case t.Search => {
            buf.+=(("其他", "搜索", event, userId, duration))
          }
          case _ => null
        }
      }
      case None => null
    }

    buf.toList

  }


}
