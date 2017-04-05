package com.moretv.bi.logETL

/**
  * Created by chubby on 2017/3/29.
  */
object RegexSets {

  // 少儿频道主页推荐位
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_1 = ("(kids_home)").r                                         // eg: pathMain->kids_home
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_2 = ("(home-kids_home)").r
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_3 = ("home\\*(classification|my_tv)\\*kids-(kids_home)").r
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_4 = ("home\\*(classification|my_tv|recommendation)\\*kids").r

  // 少儿频道列表
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_5 = ("home\\*(classification|my_tv)\\*kids-kids_home-([0-9A-Za-z_-]+)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_6 = ("(kids_home)-([0-9A-Za-z_-]+)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_7 = ("([0-9A-Za-z_-]+)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_8 = ("(.*kids_home)-([0-9A-Za-z_]+)-(search)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r

  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_9 = ("(kids_home)-(.*)-(.*)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
  val REGEX_MEDUSA_LIST_CATEGORY_KIDS_10 = ("home\\*(classification|my_tv)\\*kids-kids_home-([0-9A-Za-z_-]+)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r


  // pathSpecial信息
  val REGEX_MEDUSA_PATH_SPECIAL_1 = ("(subject|tag|star)-([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
  val REGEX_MEDUSA_PATH_SPECIAL_2 = ("(subject|tag|star)-([a-zA-Z0-9-\\u4e00-\\u9fa5]+)-([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r


  def main(args: Array[String]): Unit = {
//    val str = "kids_home-adhf*中国-dff*美团-zhong*东方-sfhf*afe"
//    val str2 = "home*classification*kids-kids_home-kandonghua*少儿精选"
//    val str3 = "home*classification*kids*动画专题"
//    val str1 = "one*fjs"
//    val reg = (".*-(.*)\\*(.*)").r
//
//    val reg1 = ("(kids_home)-(.*)-(.*)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
//
//    val reg2 = ("home(.*)-(.*)-(.*)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
//
//
//
//    reg2 findFirstMatchIn str3 match {
//      case Some(p) => {
//        (1 until p.groupCount+1).foreach(i=>{
//          println(p.group(i))
//        })
//      }
//      case None =>
//    }

//
//   val (enterWay,area,location,pageInfo,tabInfo) =  PathETLParser.pathMainParse("movie*院线大片")
//    println(s"The result is: ${enterWay} ${area} ${location} ${pageInfo} ${tabInfo}")



  }
}
