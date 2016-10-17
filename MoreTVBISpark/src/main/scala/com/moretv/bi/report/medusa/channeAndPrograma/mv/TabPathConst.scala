package com.moretv.bi.report.medusa.channeAndPrograma.mv

/**
  * Created by witnes on 10/17/16.
  */
object TabPathConst {

  // 一级目录

  val Mv_Collect = "mv\\*mineHomePage\\*site_collect".r // 我的 - 收藏

  val Mv_Recommender = "mv\\*mvRecommendHomePage\\*[a-zA-Z0-9]+".r // 推荐位 - 推荐

  val Mv_TopList = "mv\\*mvTopHomePage".r // 推荐位 - 榜单

  val Mv_Station = "mv\\*mvRecommendHomePage\\*.+-mv_station".r // 推荐位 - 电台

  val Mv_Category = "mv\\*mvCategoryHomePage".r // 推荐位 - 分类

  val Mv_Search = "mv-search".r // 入口 - 搜索

  val Mv_Singer = "mv\\*function\\*site_hotsinger".r //入口 - 歌手

  val Mv_Dance = "mv\\*function\\*site_dance".r //入口 - 舞蹈

  val Mv_Subject = "mv\\*function\\*site_mvsubject".r //入口 - 精选集

  val Mv_Concert = "mv\\*function\\*site_concert".r // 入口 - 演唱会

  val Search = "search\\*".r // 搜索

  val Mv_First_Path = Array(
    Mv_Collect,
    Mv_Station,
    Mv_Recommender,
    Mv_TopList,
    Mv_Category,
    Mv_Search,
    Mv_Singer,
    Mv_Dance,
    Mv_Subject,
    Mv_Concert,
    Search
  )
  //二级目录

  val Mv_Style = "\\*site_mvarea".r //分类 - 地区

  val Mv_Area = "\\*site_mvarea".r //分类 - 地区

  val Mv_Year = "\\*site_mvyear".r //分类 - 年代

  val Mv_Latest = "\\*xinge".r //榜单 - 新歌榜

  val Mv_Billboard = "\\*meiguogonggaopai".r //榜单 - 美国公告榜

  val Mv_Hot = "\\*rege".r //榜单 - 热歌榜

  val Mv_More = "\\*site_mvtop".r //榜单 - 更多榜单

  val Mv_Second_Path = Array(
    Mv_Style,
    Mv_Area,
    Mv_Year,
    Mv_Latest,
    Mv_Billboard,
    Mv_Hot,
    Mv_More
  )

  //三级目录

  val Mv_Sub_Category = "-mv_category\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,}".r //内部是tab, for 分类 | 演唱会 | 舞蹈

  val Mv_poster = "-mv_poster".r // 内部poster 是视频集, for 热门歌手 | 精选集

  val Mv_Third_Path = Array(
    Mv_Sub_Category,
    Mv_poster
  )

}
