package com.moretv.bi.report.medusa.channeAndPrograma.mv

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 10/18/16.
  */


object PlayPathMatch {

  def mvPathMatch(path: String, event:String,  ): List[(String, String, String, String, Long)] = {

    val t = TabPathConst

    // 我的，电台，推荐，榜单， 分类, 入口-搜索，入口-歌手，入口-舞蹈，入口-精选集，入口-演唱会
    val firstPath = t.Mv_First_Path.mkString("|")

    // 分类-风格，分类-地区，分类-年代，榜单-新歌榜，榜单-美国公告榜，榜单-热歌榜，榜单-更多榜单
    val secondPath = t.Mv_Second_Path.mkString("|")

    // 二级分类 和一级分类下的tab页面 和 视频集
    val thirdPath = t.Mv_Third_Path.mkString("|")

    val re = s"($firstPath)($secondPath)($thirdPath)"

    val buf = ListBuffer.empty[(String, String, String, String, Long)]


  }



}
