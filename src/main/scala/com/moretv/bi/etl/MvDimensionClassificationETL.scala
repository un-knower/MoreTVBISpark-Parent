package com.moretv.bi.etl

/**
  * Created by baozhi.wang on 2017/3/24.
  */
@deprecated
object MvDimensionClassificationETL {

  private val t = TabPathConstETL

  // 我的，电台，推荐，榜单， 分类, 入口-搜索，入口-歌手，入口-舞蹈，入口-精选集，入口-演唱会
  private val firstPath = t.Mv_First_Path.mkString("|")

  // 分类-风格，分类-地区，分类-年代，榜单-新歌榜，榜单-美国公告榜，榜单-热歌榜，榜单-更多榜单
  private val secondPath = t.Mv_Second_Path.mkString("|")

  // 二级分类 和一级分类下的tab页面 和 视频集
  private val thirdPath = t.Mv_Third_Path.mkString("|")

  private val re = s"($firstPath)($secondPath)?($thirdPath)?".r


  def mvPathMatch(path: String, index: Int): String = {
    var result: String = ""
    re findFirstMatchIn path match {
      case Some(p) => {
        if (t.Mv_Collect.pattern.matcher(p.group(1)).matches) {
          if (p.group(3) != null && t.Mv_collection.pattern.matcher(p.group(3)).matches) {
            if (index == 1) {
              result = "音乐入口"
            }else if (index == 2) {
              result = "我的收藏"
            }
          }
        }

        else if (t.Mv_Station.pattern.matcher(p.group(1)).matches) {
          if (index == 1) {
            result = "音乐入口"
          }else if (index == 2) {
            result = "电台"
          }
        }

        else if (t.Mv_Recommender.pattern.matcher(p.group(1)).matches) {
          if (index == 1) {
            result = "音乐入口"
          }else if (index == 2) {
            result = "推荐"
          }
        }

        else if (t.Mv_TopList.pattern.matcher(p.group(1)).matches) {
          if (p.group(2) != null && t.Mv_Latest.pattern.matcher(p.group(2)).matches) {
            if (index == 1) {
              result = "音乐入口"
            }else if (index == 2) {
              result = "榜单"
            } else if (index == 3) {
              result = "新歌榜"
            }
          }
          else if (p.group(2) != null && t.Mv_Billboard.pattern.matcher(p.group(2)).matches) {
            if (index == 1) {
              result = "音乐入口"
            }else if (index == 2) {
              result = "榜单"
            } else if (index == 3) {
              result = "美国公告榜"
            }
          }
          else if (p.group(2) != null && t.Mv_Hot.pattern.matcher(p.group(2)).matches) {
            if (index == 1) {
              result = "音乐入口"
            }else if (index == 2) {
              result = "榜单"
            } else if (index == 3) {
              result = "热歌榜"
            }
          }
          else if (p.group(2) != null && t.Mv_Rise.pattern.matcher(p.group(2)).matches) {
            if (index == 1) {
              result = "音乐入口"
            }else if (index == 2) {
              result = "榜单"
            } else if (index == 3) {
              result = "飙升榜"
            }
          }
          else if (p.group(2) != null && t.Mv_More.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_poster.pattern.matcher(p.group(3)).matches) {
              if (index == 1) {
                result = "音乐入口"
              }else if (index == 2) {
                result = "榜单"
              } else if (index == 3) {
                result = "更多榜单"
              }
            }
          }
        }

        else if (t.Mv_Search.pattern.matcher(p.group(1)).matches) {
           if (index == 1) {
            result = "音乐入口"
          } else if (index == 2) {
            result = "音乐搜索"
          }
        }

        else if (t.Mv_Singer.pattern.matcher(p.group(1)).matches) {
          if (index == 1) {
            result = "音乐入口"
          } else if (index == 2) {
            result = "热门歌手"
          }
        }

        else if (t.Mv_Concert.pattern.matcher(p.group(1)).matches) {
          if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
            if (index == 1) {
              result = "音乐入口"
            } else if (index == 2) {
              result = "演唱会"
            }else if (index == 3){
              //华语,港台,欧美,二次元,韩国,日本,其他
              result = p.group(3).split("\\*")(1)
            }
          }
        }

        else if (t.Mv_Dance.pattern.matcher(p.group(1)).matches) {

          if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
            if (index == 1) {
              result = "音乐入口"
            } else if (index == 2) {
              result = "舞蹈"
            }else if (index == 3) {
              //宅舞,三次元舞蹈,舞蹈教程
              result = p.group(3).split("\\*")(1)
            }
          }
        }

        else if (t.Mv_Subject.pattern.matcher(p.group(1)).matches) {
          if (p.group(3) != null && t.Mv_poster.pattern.matcher(p.group(3)).matches) {
            if (index == 1) {
              result = "音乐入口"
            } else if (index == 2) {
              result = "精选集"
            }
          }

        }

       /* else if (t.Search.pattern.matcher(p.group(1)).matches) {
          if (index == 1) {
            result = "音乐入口"
          } else if (index == 2) {
            result = "搜索"
          }
        }*/

        else if (t.Mv_Category.pattern.matcher(p.group(1)).matches) {
          if (p.group(2) != null && t.Mv_Style.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
              if(index == 1){
                result = "音乐分类"
              }else if (index == 2) {
                result = "风格"
              } else if (index == 3) {
                result = p.group(3).split("\\*")(1)
              }
            }
          }
          if (p.group(2) != null && t.Mv_Year.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
              if(index == 1){
                result = "音乐分类"
              }else if (index == 2) {
                result = "年代"
              } else if (index == 3) {
                result = p.group(3).split("\\*")(1)
              }
            }

          }
          if (p.group(2) != null && t.Mv_Area.pattern.matcher(p.group(2)).matches) {
            if (p.group(3) != null && t.Mv_Sub_Category.pattern.matcher(p.group(3)).matches) {
              if(index == 1){
                result = "音乐分类"
              }else if (index == 2) {
                result = "地区"
              } else if (index == 3) {
                result = p.group(3).split("\\*")(1)
              }
            }
          }
        }
      }
      case None =>
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val list=List("home*classification*mv-mv*mvRecommendHomePage*qrwy4fc39xnp-mv_station","home*my_tv*mv-mv*电台","home*classification*mv-mv*电台","home*classification*mv-mv*mvCategoryHomePage*site_mvyear-mv_category*90年代","home*classification*mv-mv*function*site_dance-mv_category*宅舞","home*classification*mv-mv-search*GZDZ","home*classification*mv-mv*function*site_concert-mv_category*港台","home*classification*mv-mv-search*SYMP")
      for (path <- list){
        val str1=mvPathMatch(path,1)
        val str2=mvPathMatch(path,2)
        val str3=mvPathMatch(path,3)
        println(s"path:$path,str1:$str1,str2:$str2,str3:$str3")
      }
  }
}
