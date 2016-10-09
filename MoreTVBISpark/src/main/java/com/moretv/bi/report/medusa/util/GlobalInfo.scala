package com.moretv.bi.report.medusa.util

/**
 * Created by xiajun on 2016/5/9.
 * This object is used to define the global info!
 */
object GlobalInfo {

//  该变量定义了目前业务中所涉及的路径的字段
  val PATHFIELDS = new PathField("path","pathMain","pathSub","pathSpecial")

//  该变量定义了目前业务中所需输出的字段信息
  val OUTPUTFIELDS = new OutputField("launcherAccessArea","homeAccessLocation","pageType","pageTabInfo","accessPath",
    "currentSid","upSid","upContentType","pathProperty","pathFlag")

  /**
   * 该变量记录的是launcher页面的不同访问区域块
   * navi包含了“搜索”和“设置”两块
   * classification表示“分类”
   * my_tv表示“我的电视”
   * live表示“直播频道”
   * recommendation表示“今日推荐”
   * foundation表示“发现”
   */
  val LAUNCHERACCESSAREA = Array("classification","my_tv","live","recommendation","foundation","navi")

  val ACCESSLOCATION = Array("history","collect","account","movie","tv","zongyi","jilu","comic","xiqu","kids","hot","mv",
    "top_new","top_hot","top_star","top_collect","interest_location","0","1","2","3","4","5","6","7","8","9","10","11",
    "12","13","14")

  val PAGETYPE =Array("history","accountcenter_home","movie","tv","zongyi","jilu","comic","xiqu","mv","hot","kids_collect",
    "kids_anim","kids_rhymes","everyone_watching","rank")

  /**
   * 此处定义了一些moretv日志中所需要的一些解析信息
   */
  val MORETVNAVI = Array("search","setting")
  val MORETVCLASSIFICATION = Array("history","movie","tv","live","hot","zongyi","kids_home","comic","mv","jilu","xiqu",
    "sports","subject")
  val MORETVWATCHHISTORY = "watchhistory"
  val MORETVOTHERWATCH ="otherwatch"
  val MORETVRECOMMEND = "hotrecommend"
  val MORETVWEISHILIVE = "TVlive"
  val MORETVPATHSUBCATEGORY = Array("similar","peoplealsolike","actor")
  val MORETVPATHSPECIALCATEGORY = Array("tag","subject","star")
  val CONTENTTYPE = Array("history","movie","tv","zongyi","hot","comic","mv","xiqu","sports","jilu","subject")
  val MORETVPAGETABINFOFOUR = Array("kids_home")
  val MORETVPAGETABINFOTHREE = Array("history","movie","tv","zongyi","hot","comic","mv","xiqu","jilu","subject")

}


/**
 * 该类定义了业务中所涉及的关于路径的字段信息
 *
 */
case class PathField(field:String*){
  val PATH = field(0)       //moretv的path字段
  val PATHMAIN = field(1)         //medusa的pathMain字段
  val PATHSUB = field(2)      //medusa的pathSub字段
  val PATHSPECIAL = field(3)       //medusa的pathSpecial字段
}

/**
 * 该类定义了业务中输出字段信息
 *
 */
case class OutputField(field:String*){
  val LAUNCHERACCESSAREA = field(0)
  val LAUNCHERACCESSLOCATION = field(1)
  val PAGETYPE = field(2)
  val PAGETABINFO = field(3)
  val ACCESSPATH = field(4)
  val CURRENTSID = field(5)
  val UPSID = field(6)
  val UPCONTENTTYPE = field(7)
  val PATHPROPERTY = field(8)
  val PATHFLAG = field(9)
}


