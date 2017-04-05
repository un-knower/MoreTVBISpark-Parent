package com.moretv.bi.logETL

/**
  * Created by chubby on 2017/3/29.
  */
object Regexs {


  val LIST_CATEGORY_REGEX = ("([a-zA-Z0-9_]+)\\*([\\u4e00-\\u9fa5A-Z0-9]+)").r                // 获取列表信息
  val ENTER_WAY_REGEX = ("^(home)(.*)").r                                                       // 确定是否app应用内还是第三方跳转
  val KIDS_3X_RECOMMEND_REGEX = ("(.*)(kids_home|kids)$").r


  val LIST_CATEGORY_REGEX_3X_1 = ("(.*)\\*([\\u4e00-\\u9fa5]+)").r                // 主要处理以列表页中文名结束的路径
  val LIST_CATEGORY_REGEX_3X_12 = ("(.*)-(.*)\\*([\\u4e00-\\u9fa5]+)").r
  val LIST_CATEGORY_REGEX_3X_13 = ("(.*)-(.*)").r




  // 本应用类行为路径,四级分段分析
  val LIST_CATEGORY_REGEX_3X_3 = ("^home(-|\\*)(.*)-(.*)\\*([\\u4e00-\\u9fa5]+)$").r     //路径需以站点树结尾

  // 第三方应用跳转路径解析,二级分段分析


  def main(args: Array[String]): Unit = {
//    val str = "kids_collect*消息中心"
    val str = "home*classification*kids"



    KIDS_3X_RECOMMEND_REGEX findFirstMatchIn str match {
      case Some(p) => {
        (1 until p.groupCount+1).foreach(i=>{
          println(p.group(i))
        })
      }
      case None =>
    }
  }
//
//  val (pageInfo,tabInfo) = PathETLParser.parseListCategory("sd*张")
//  println(pageInfo)
//  println(tabInfo)

}
