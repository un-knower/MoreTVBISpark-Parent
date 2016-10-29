package com.moretv.bi.report.medusa.channeAndPrograma.mv

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 10/18/16.
  */


object PlayPathMatch {


  private val t = TabPathConst

  // 我的，电台，推荐，榜单， 分类, 入口-搜索，入口-歌手，入口-舞蹈，入口-精选集，入口-演唱会
  private val firstPath = t.Mv_First_Path.mkString("|")

  // 分类-风格，分类-地区，分类-年代，榜单-新歌榜，榜单-美国公告榜，榜单-热歌榜，榜单-更多榜单
  private val secondPath = t.Mv_Second_Path.mkString("|")

  // 二级分类 和一级分类下的tab页面 和 视频集
  private val thirdPath = t.Mv_Third_Path.mkString("|")

  val re = s"($firstPath)($secondPath)?($thirdPath)?".r

  /**
    *
    * @param path
    * @param event
    * @param userId
    * @param duration
    * @return
    */
  def mvPathMatch(path: String, event: String, userId: String, duration: Long)
  : List[(String, String, String, String, Long)] = {

    val buf = ListBuffer.empty[(String, String, String, String, Long)]

    re findFirstMatchIn path match {

      case Some(p) => {

        if (t.Mv_Collect.pattern.matcher(p.group(1)).matches) {
          if (p.group(3) != null && t.Mv_collection.pattern.matcher(p.group(3)).matches) {
            buf.+=(("我的", "收藏", event, userId, duration))
          }
        }

        else if (t.Mv_Station.pattern.matcher(p.group(1)).matches) {
          buf.+=(("电台", "电台", event, userId, duration))
        }

        else if (t.Mv_Recommender.pattern.matcher(p.group(1)).matches) {
          buf.+=(("推荐", "推荐", event, userId, duration))
        }

        else if (t.Mv_TopList.pattern.matcher(p.group(1)).matches) {
          if (p.group(2) != null && t.Mv_Latest.pattern.matcher(p.group(2)).matches) {

            buf.+=(("榜单", "榜单", event, userId, duration))
            buf.+=(("榜单", "新歌榜", event, userId, duration))

          }
          else if (p.group(2) != null && t.Mv_Billboard.pattern.matcher(p.group(2)).matches) {

            buf.+=(("榜单", "榜单", event, userId, duration))
            buf.+=(("榜单", "美国公告榜", event, userId, duration))

          }
          else if (p.group(2) != null && t.Mv_Hot.pattern.matcher(p.group(2)).matches) {

            buf.+=(("榜单", "榜单", event, userId, duration))
            buf.+=(("榜单", "热歌榜", event, userId, duration))

          }
          else if (p.group(2) != null && t.Mv_More.pattern.matcher(p.group(2)).matches) {

            if (p.group(3) != null && t.Mv_poster.pattern.matcher(p.group(3)).matches) {

              buf.+=(("榜单", "榜单", event, userId, duration))
              buf.+=(("榜单", "更多榜单", event, userId, duration))

            }

          }
        }

        else if (t.Mv_Search.pattern.matcher(p.group(1)).matches) {
          buf.+=(("入口", "音乐搜索", event, userId, duration))
        }

        else if (t.Mv_Singer.pattern.matcher(p.group(1)).matches) {
          buf.+=(("入口", "歌手", event, userId, duration))
        }

        else if (t.Mv_Concert.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

            buf.+=(("入口", "演唱会", event, userId, duration))
            buf.+=(("演唱会", p.group(3).split("\\*")(1), event, userId, duration))

          }
        }

        else if (t.Mv_Dance.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

            buf.+=(("入口", "舞蹈", event, userId, duration))
            buf.+=(("舞蹈", p.group(3).split("\\*")(1), event, userId, duration))

          }
        }

        else if (t.Mv_Subject.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_poster.pattern.matcher(p.group(3)).matches) {

            buf.+=(("入口", "精选集", event, userId, duration))

          }

        }

        else if (t.Search.pattern.matcher(p.group(1)).matches) {

          buf.+=(("其他", "搜索", event, userId, duration))

        }

        else if (t.Mv_Category.pattern.matcher(p.group(1)).matches) {
          if (p.group(2) != null && t.Mv_Style.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
              buf.+=(("分类", "分类", event, userId, duration))
              buf.+=(("分类", "风格", event, userId, duration))
              buf.+=(("分类", p.group(3).split("\\*")(1), event, userId, duration))
            }

          }
          if (p.group(2) != null && t.Mv_Year.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
              buf.+=(("分类", "分类", event, userId, duration))
              buf.+=(("分类", "年代", event, userId, duration))
              buf.+=(("分类", p.group(3).split("\\*")(1), event, userId, duration))
            }

          }
          if (p.group(2) != null && t.Mv_Area.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
              buf.+=(("分类", "分类", event, userId, duration))
              buf.+=(("分类", "地区", event, userId, duration))
              buf.+=(("分类", p.group(3).split("\\*")(1), event, userId, duration))
            }

          }
        }

      }

      case None => null
    }

    buf.toList

  }


  /**
    *
    * @param path
    * @param videoSid
    * @param videoName
    * @param event
    * @param userId
    * @param duration
    * @return
    */
  def mvPathMatch(path: String, videoSid: String, videoName: String, event: String, userId: String, duration: Long)
  : List[(String, String, String, String, String, Long)] = {

    val buf = ListBuffer.empty[(String, String, String, String, String, Long)]

    re findFirstMatchIn path match {

      case Some(p) => {

        if (t.Mv_Collect.pattern.matcher(p.group(1)).matches) {
          if (p.group(3) != null && t.Mv_collection.pattern.matcher(p.group(3)).matches) {
            buf.+=(("音乐收藏", videoSid, videoName, event, userId, duration))
          }
        }

        else if (t.Mv_Station.pattern.matcher(p.group(1)).matches) {
          buf.+=(("电台", videoSid, videoName, event, userId, duration))
        }

        else if (t.Mv_Recommender.pattern.matcher(p.group(1)).matches) {
          buf.+=(("推荐", videoSid, videoName, event, userId, duration))
        }

        else if (t.Mv_TopList.pattern.matcher(p.group(1)).matches) {
          if (p.group(2) != null && t.Mv_Latest.pattern.matcher(p.group(2)).matches) {

            buf.+=(("榜单", videoSid, videoName, event, userId, duration))
            //buf.+=(("新歌声", videoSid, videoName, event, userId, duration))

          }
          else if (p.group(2) != null && t.Mv_Billboard.pattern.matcher(p.group(2)).matches) {

            buf.+=(("榜单", videoSid, videoName, event, userId, duration))
            //    buf.+=(("美国公告榜", videoSid, videoName, event, userId, duration))

          }
          else if (p.group(2) != null && t.Mv_Hot.pattern.matcher(p.group(2)).matches) {

            buf.+=(("榜单", videoSid, videoName, event, userId, duration))
            //  buf.+=(("热歌榜", videoSid, videoName, event, userId, duration))

          }
          else if (p.group(2) != null && t.Mv_More.pattern.matcher(p.group(2)).matches) {

            if (p.group(3) != null && t.Mv_poster.pattern.matcher(p.group(3)).matches) {

              buf.+=(("榜单", videoSid, videoName, event, userId, duration))
              //     buf.+=(("更多榜单", videoSid, videoName, event, userId, duration))

            }

          }
        }

        else if (t.Mv_Search.pattern.matcher(p.group(1)).matches) {
          buf.+=(("音乐搜索", videoSid, videoName, event, userId, duration))
        }

        else if (t.Mv_Singer.pattern.matcher(p.group(1)).matches) {
          buf.+=(("歌手", videoSid, videoName, event, userId, duration))
        }

        else if (t.Mv_Concert.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

            buf.+=(("演唱会", videoSid, videoName, event, userId, duration))
            //  buf.+=(("演唱会", p.group(3).split("\\*")(1), videoSid, videoName, event, userId, duration))

          }
        }

        else if (t.Mv_Dance.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

            buf.+=(("舞蹈", videoSid, videoName, event, userId, duration))
            //   buf.+=(("舞蹈", p.group(3).split("\\*")(1), videoSid, videoName, event, userId, duration))

          }
        }

        else if (t.Mv_Subject.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_poster.pattern.matcher(p.group(3)).matches) {

            buf.+=(("精选集", videoSid, videoName, event, userId, duration))

          }

        }

        else if (t.Search.pattern.matcher(p.group(1)).matches) {

          buf.+=(("其它搜索", videoSid, videoName, event, userId, duration))

        }

        else if (t.Mv_Category.pattern.matcher(p.group(1)).matches) {
          if (p.group(2) != null && t.Mv_Style.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

              buf.+=(("分类", videoSid, videoName, event, userId, duration))
              //  buf.+=(("分类", "风格", videoSid, videoName, event, userId, duration))
              //  buf.+=(("分类", p.group(3).split("\\*")(1), videoSid, videoName, event, userId, duration))

            }

          }
          if (p.group(2) != null && t.Mv_Year.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

              buf.+=(("分类", videoSid, videoName, event, userId, duration))
              //   buf.+=(("分类", "年代", videoSid, videoName, event, userId, duration))
              //   buf.+=(("分类", p.group(3).split("\\*")(1), videoSid, videoName, event, userId, duration))

            }

          }
          if (p.group(2) != null && t.Mv_Area.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {

              buf.+=(("分类", videoSid, videoName, event, userId, duration))
              //    buf.+=(("分类", "地区", videoSid, videoName, event, userId, duration))
              //    buf.+=(("分类", p.group(3).split("\\*")(1), videoSid, videoName, event, userId, duration))

            }

          }
        }

      }

      case None => null
    }

    buf.toList

  }


}
