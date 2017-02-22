package com.moretv.bi.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

/**
 * Created by laishun on 2015/7/17.
 */
object CodeToNameUtils {
  /**
   * 定义一些map集合
   */
//   var sidApplicationNameMap = Map[String,String]()
//   var sidSubjectNameMap = Map[String,String]()
//   lazy val subjectName2CodeMap = Map[String,String]()
//   var thirdPaheNameMap = Map[String,String]()
//   var subCodeToParentNameMap = Map[String,String]()
//   var channelNameMap = Map[String,String]()
//   var subChannelNameMap = Map[String,String]()
//   var sidProgramNameMap = Map[String,String]()
//   var sidProgramDurationMap = Map[String,String]()
  lazy val sidApplicationNameMap = initSidMapInfo(url_tvservice3_1, sid2ApplicationNameMapSql)
  lazy val sidSubjectNameMap = initSidMapInfo(url_tvservice3_1, sid2SubjectNameMapSql)
  lazy val subjectName2CodeMap = initSidMapInfo(url_tvservice3_1, subjectName2CodeMapSql)
  var thirdPaheNameMap = Map[String,String]()
  var subCodeToParentNameMap = Map[String,String]()
  var channelNameMap = Map[String,String]()
  var subChannelNameMap = Map[String,String]()
  lazy val sidProgramNameMap = initSidMapInfo(url_tvservice3_1,programSid2NameMapSql)
  lazy val sidProgramDurationMap = initSidMapInfo(url_tvservice3_1,programSid2DurationMapSql)
  /**
   * 定义一些常量
   */
   val driver:String = "com.mysql.jdbc.Driver"
   val  user:String = "bi"
   val password:String = "mlw321@moretv"

   val url_tvservice3_1:String = "jdbc:mysql://10.10.2.19:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
   val url_bi2_1:String = "jdbc:mysql://10.10.2.15:3306/bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"

   val sid2ApplicationNameMapSql:String = "SELECT sid,title FROM tvservice.mtv_application WHERE sid IS NOT NULL"
   val sid2SubjectNameMapSql:String = "SELECT code,name FROM tvservice.mtv_subject WHERE code IS NOT NULL"
   val subjectName2CodeMapSql:String = "SELECT name,code FROM tvservice.mtv_subject WHERE name IS NOT NULL"
   val thirdPathSql:String = "SELECT code,name FROM bi.mtv_list"
   val subCodeToParentNameSql:String = "SELECT code,parent_code FROM bi.mtv_list where hierarchy = 3"
   val channelNameSql:String = "SELECT sid,station FROM tvservice.mtv_channel where sid is not null"
   val subChannelNameSql:String = "SELECT code,name FROM tvservice.mtv_position"
   val programSid2NameMapSql:String = "SELECT sid,title FROM tvservice.mtv_program WHERE sid IS NOT NULL"
   val programSid2DurationMapSql:String = "SELECT sid,duration FROM tvservice.mtv_program WHERE sid IS NOT NULL"
  /**
   * Function: obtain data from data table
   * @param url
   * @param sql
   * @param map
   */
  def initSidMap(url: String, sql: String, map: Map[String, String]) ={
    try {
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, user, password)
      val stat: Statement = conn.createStatement
      val rs: ResultSet = stat.executeQuery(sql)
      while (rs.next) {
        map +=(rs.getString(1) -> rs.getString(2))
      }
      rs.close
      stat.close
      conn.close
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(e)
      }
    }
  }

  /**
    * 使用懒加载的方式进行初始化
    */
  private def initSidMapInfo(url:String,sql:String,userName:String = user,passwordStr:String = password) = {
    Class.forName(driver)
    val conn:Connection = DriverManager.getConnection(url,userName,passwordStr)
    try{
      val stat:Statement = conn.createStatement
      val rs:ResultSet = stat.executeQuery(sql)
      val map = new mutable.HashMap[String,String]()
      while(rs.next()){
        map.+= (rs.getString(1) -> rs.getString(2))
      }
      rs.close()
      stat.close()
      conn.close()
      map.toMap
    }finally {
      conn.close()
    }
  }
  /**
   * Function: obtain subject name by subject code
   * @param sid
   * @return
   */
  def getSubjectNameBySid(sid: String):String ={
//    if(sidSubjectNameMap.isEmpty){
//      initSidMap(url_tvservice3_1, sid2SubjectNameMapSql, sidSubjectNameMap)
//    }
    sidSubjectNameMap.getOrElse(sid,sid)
  }

  def getAllSubjectName:collection.Map[String,String] = {
//    if(sidSubjectNameMap.isEmpty){
//      initSidMap(url_tvservice3_1, sid2SubjectNameMapSql, sidSubjectNameMap)
//    }
    sidSubjectNameMap
  }

  /**
   * Function: obtain program name by program sid
   * @param sid
   * @return
   */

  def getProgramNameBySid(sid:String):String = {
//    if(sidProgramNameMap.isEmpty){
//      initSidMap(url_tvservice3_1,programSid2NameMapSql,sidProgramNameMap)
//    }
    sidProgramNameMap.getOrElse(sid,"null")
  }

  /**
    * Function: obtain subject name by subject code
    * @param name
    * @return
    */
  def getSubjectCodeByName(name: String):String ={
//    if(subjectName2CodeMap.isEmpty){
//      initSidMap(url_tvservice3_1, subjectName2CodeMapSql, subjectName2CodeMap)
//    }
    subjectName2CodeMap.getOrElse(name,"null")
  }

  def getAllSubjectCode():collection.Map[String,String] = {
//    if(subjectName2CodeMap.isEmpty){
//      initSidMap(url_tvservice3_1, subjectName2CodeMapSql, subjectName2CodeMap)
//    }
    subjectName2CodeMap
  }

  /**
   * Function: obtain apllication name by sid
   * @param sid
   * @return
   */
  def getApplicationNameBySid(sid: String):String ={
//    if(sidApplicationNameMap.isEmpty){
//      initSidMap(url_tvservice3_1, sid2ApplicationNameMapSql, sidApplicationNameMap)
//    }
    sidApplicationNameMap.getOrElse(sid,"null")
  }

  /**
   * Function: obtain path name by path code
   * @param sid
   * @return
   */
  def getThirdPathName(sid:String):String ={
    if(thirdPaheNameMap.isEmpty){
      initSidMap(url_bi2_1, thirdPathSql, thirdPaheNameMap)
    }
    thirdPaheNameMap.getOrElse(sid,"null")
  }

  /**
   * Function: obtain channel name by sid
   * @param sid
   * @return
   */
  def getChannelNameBySid(sid:String):String ={
    if(channelNameMap.isEmpty){
      initSidMap(url_tvservice3_1, channelNameSql, channelNameMap)
    }
    channelNameMap.getOrElse(sid,null)
  }

  /**
   * Function: obtain the name of parent's path by sub path code
   * @param code
   * @return
   */
  def getParentNameBySubCode(code:String): String ={
    if(subCodeToParentNameMap.isEmpty){
      initSidMap(url_bi2_1, subCodeToParentNameSql, subCodeToParentNameMap)
    }
    var parentName = subCodeToParentNameMap.getOrElse(code,"")
    if("".equalsIgnoreCase(parentName)){
      parentName = code
    }
    parentName
  }

  /**
   * Function:
   * @return
   */
  def getSportsSubsectionList() = {
    val sectionList = new ListBuffer[String]()
    try {
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url_bi2_1, user, password)
      val stat: Statement = conn.createStatement
      val sectionSql = "select code from mtv_list where hierarchy=1 and parent_code='sports'"
      val rs: ResultSet = stat.executeQuery(sectionSql)
      while (rs.next) {
        sectionList += rs.getString(1)
      }
      rs.close
      stat.close
      conn.close
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(e)
      }
    }
    sectionList
  }

  def getProgramDurationFromSid(sid:String) ={
//    if(sidProgramDurationMap.isEmpty){
//      initSidMap(url_tvservice3_1,programSid2DurationMapSql,sidProgramDurationMap)
//    }
    sidProgramDurationMap.getOrElse(sid,"null")
  }

  def getSubChannelNameByCode(code:String):String ={
    if(subChannelNameMap.isEmpty){
      initSidMap(url_tvservice3_1, subChannelNameSql, subChannelNameMap)
      subChannelNameMap+=("movie_hot"->"院线大片")
      subChannelNameMap+=("tv_genbo"-> "华语热播")
      subChannelNameMap+=("p_zongyi_hot_1"-> "综艺热播")
      subChannelNameMap+=("p_shaoer_hot_1"-> "少儿热播")
      subChannelNameMap+=("hot_comic_top"-> "热播动漫")
      subChannelNameMap+=("p_document_1"-> "纪实热播")
      subChannelNameMap+=("1_movie_tag_jilupiain"-> "纪录电影")
      subChannelNameMap+=("zongyi_zhuanti"-> "综艺专题")
      subChannelNameMap+=("movie_7days"-> "七日更新")
      subChannelNameMap+=("movie"-> "猜你喜欢")
      subChannelNameMap+=("movie_zhuanti"-> "电影专题")
      subChannelNameMap+=("movie_linghuo"-> "每周推荐")
      subChannelNameMap+=("movie_hollywood"-> "好莱坞巨制")
      subChannelNameMap+=("movie_huayu"-> "华语精选")
      subChannelNameMap+=("movie_yazhou"-> "日韩亚太")
      subChannelNameMap+=("bd_movie"-> "蓝光专区")
      subChannelNameMap+=("1_movie_tag_dongzuo"-> "犀利动作")
      subChannelNameMap+=("1_movie_tag_lunli"-> "人性伦理")
      subChannelNameMap+=("movie_yueyu"-> "粤语佳片")
      subChannelNameMap+=("tv_guowai_lianzai"-> "海外同步")
      subChannelNameMap+=("tv_zhuanti"-> "电视剧专题")
      subChannelNameMap+=("1_tv_area_neidi"-> "大陆剧场")
      subChannelNameMap+=("1_tv_area_xianggang"-> "香港TVB")
      subChannelNameMap+=("1_tv_area_oumei"-> "特色美剧")
      subChannelNameMap+=("1_tv_area_yingguo"-> "英伦佳剧")
      subChannelNameMap+=("1_tv_area_hanguo"-> "韩剧热流")
      subChannelNameMap+=("1_tv_area_riben"-> "日剧集锦")
      subChannelNameMap+=("1_tv_area_taiwan"-> "台湾剧集")
      subChannelNameMap+=("1_tv_area_qita"-> "其他地区")
      subChannelNameMap+=("tv_yueyu"-> "粤语专区")
      subChannelNameMap+=("movie_star"-> "影人专区")
      subChannelNameMap+=("movie_comic"-> "动画电影")
      subChannelNameMap+=("movie_yugao"-> "电影预告")
      subChannelNameMap+=("tv"-> "猜你喜欢")
      subChannelNameMap+=("zongyi_weishi"-> "卫视强档")
      subChannelNameMap+=("1_zongyi_area_hanguo"-> "韩国综艺")
      subChannelNameMap+=("1_zongyi_tag_zhenrenxiu"-> "真人秀场")
      subChannelNameMap+=("1_zongyi_tag_fangtan"-> "情感访谈")
      subChannelNameMap+=("1_zongyi_tag_youxi"-> "游戏竞技")
      subChannelNameMap+=("1_zongyi_tag_gaoxiao"-> "爆笑搞怪")
      subChannelNameMap+=("1_zongyi_tag_gewu"-> "歌舞晚会")
      subChannelNameMap+=("1_zongyi_tag_shenghuo"-> "时尚生活")
      subChannelNameMap+=("1_zongyi_tag_quyi"-> "说唱艺术")
      subChannelNameMap+=("1_zongyi_tat_caijing"-> "财经民生")
      subChannelNameMap+=("1_zongyi_tag_fazhi"-> "社会法制")
      subChannelNameMap+=("1_zongyi_tag_bobao"-> "新闻播报")
      subChannelNameMap+=("1_zongyi_tag_qita"-> "其他分类")
      subChannelNameMap+=("hanguo_jingxuan"-> "韩国精选")
      subChannelNameMap+=("gangtai_jingxuan"-> "港台精选")
      subChannelNameMap+=("jilu_meishi"-> "美食大赏")
      subChannelNameMap+=("dianshiju_tuijain"-> "卫视好剧大放送")
      subChannelNameMap+=("p_donghua1"-> "适合2-5岁")
      subChannelNameMap+=("p_donghua2"-> "适合5-8岁")
      subChannelNameMap+=("p_donghua3"-> "适合8-12岁")
      subChannelNameMap+=("1_kids_tags_shaoerpingdao"-> "少儿动画")
      subChannelNameMap+=("1_kids_tags_qingzi"-> "亲子交流")
      subChannelNameMap+=("1_kids_tags_yizhi"-> "益智启蒙")
      subChannelNameMap+=("1_kids_tags_dongwu"-> "动物乐园")
      subChannelNameMap+=("1_kids_tags_tonghua"-> "童话故事")
      subChannelNameMap+=("1_kids_tags_yuer"-> "少儿教育")
      subChannelNameMap+=("1_kids_tags_donghuadianyin"-> "动画电影")
      subChannelNameMap+=("comic_zhujue"-> "动漫主角")
      subChannelNameMap+=("dongman_xinfan"-> "新番上档")
      subChannelNameMap+=("1_comic_tags_jizhang"-> "机甲战斗")
      subChannelNameMap+=("1_comic_tags_rexue"-> "热血冒险")
      subChannelNameMap+=("1_comic_tags_meishaonu"-> "后宫萝莉")
      subChannelNameMap+=("1_comic_tags_qingchun"-> "青春浪漫")
      subChannelNameMap+=("1_comic_tags_gaoxiao"-> "轻松搞笑")
      subChannelNameMap+=("1_comic_tags_lizhi"-> "励志治愈")
      subChannelNameMap+=("1_comic_tags_huanxiang"-> "奇幻魔法")
      subChannelNameMap+=("1_comic_tags_xuanyi"-> "悬疑推理")
      subChannelNameMap+=("1_comic_tags_qita"-> "其他分类")
      subChannelNameMap+=("1_jilu_station_bbc"-> "BBC")
      subChannelNameMap+=("1_jilu_station_ngc"-> "国家地理")
      subChannelNameMap+=("1_jilu_station_nhk"-> "NHK")
      subChannelNameMap+=("1_jilu_tags_ted"-> "公开课")
      subChannelNameMap+=("1_jilu_tags_junshi"-> "军事风云")
      subChannelNameMap+=("1_jilu_tags_ziran"-> "自然万象")
      subChannelNameMap+=("1_jilu_tags_shehui"-> "社会百态")
      subChannelNameMap+=("1_jilu_tags_renwu"-> "人物大观")
      subChannelNameMap+=("1_jilu_tags_lishi"-> "历史钩沉")
      subChannelNameMap+=("1_jilu_tags_muhou"-> "幕后故事")
      subChannelNameMap+=("1_jilu_tags_keji"-> "前沿科技")
      subChannelNameMap+=("1_jilu_tags_qita"-> "其他分类")
      subChannelNameMap+=("dalu_jingxuan"-> "大陆精选")
      subChannelNameMap+=("1_xiqu_tag_guangchangwu"-> "广场舞")
      subChannelNameMap+=("1_tv_xiqu_tag_jingju"-> "京剧")
      subChannelNameMap+=("1_tv_xiqu_tag_yuju"-> "豫剧")
      subChannelNameMap+=("1_tv_xiqu_tag_yueju"-> "越剧")
      subChannelNameMap+=("1_tv_xiqu_tag_huju"-> "沪剧")
      subChannelNameMap+=("1_tv_xiqu_tag_wuju"-> "婺剧")
      subChannelNameMap+=("1_tv_xiqu_tag_huangmeixi"-> "黄梅戏")
      subChannelNameMap+=("1_tv_xiqu_tag_meihuadagu"-> "梅花大鼓")
      subChannelNameMap+=("1_xiqu_tag_suzhoutanchang"-> "苏州弹唱")
      subChannelNameMap+=("1_xiqu_tag_shanghaishuocang"-> "上海说唱")
      subChannelNameMap+=("hot_rebo"-> "今日热点")
      subChannelNameMap+=("1_hot_tag_xinwenredian"-> "新闻热点")
      subChannelNameMap+=("1_hot_tag_yulebagua"-> "娱乐八卦")
      subChannelNameMap+=("1_hot_tag_qingsonggaoxiao"-> "轻松搞笑")
      subChannelNameMap+=("1_hot_tag_tiyukuaibao"-> "体育快报")
      subChannelNameMap+=("1_hot_tag_junshiguancha"-> "军事观察")
      subChannelNameMap+=("1_hot_tag_caijingxinxi"-> "财经信息")
      subChannelNameMap+=("1_hot_tag_meishaonv"-> "美女")
      subChannelNameMap+=("1_hot_tag_lvyouguanguang"-> "旅游观光")
      subChannelNameMap+=("1_hot_tag_qiche"-> "汽车情报")
      subChannelNameMap+=("1_hot_tag_kejixinzhi"-> "科技新知")
      subChannelNameMap+=("1_hot_tag_youxi"-> "游戏世界")
      subChannelNameMap+=("kids_songhome"-> "听儿歌")
      subChannelNameMap+=("kids_seecartoon"-> "看动画")
      subChannelNameMap+=("kids_recommend"-> "少儿首页推荐位")
      subChannelNameMap+=("kids_channel"-> "看频道")
    }

    subChannelNameMap.getOrElse(code, null)
  }

  def getSubjectCodeMap:scala.collection.immutable.Map[String,String] = {
    subjectName2CodeMap
  }

}
