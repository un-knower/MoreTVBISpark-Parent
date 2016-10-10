package cn.whaley.bi.utils

import org.json.JSONObject
import scala.collection.mutable.Map

/**
 * Created by laishun on 15/12/2.
 */
object PathCodeToNameUtil {

  val code_path_name_map:Map[String,Map[String,String]] = Map()
  val kids_code_path_name_map:Map[String,Map[String,String]] = Map()
  val sport_code_path_name_map:Map[String,Map[String,String]] = Map()
  val sport_code_four_path_name_map:Map[String,Map[String,Map[String,String]]] = Map()

  /**
   *
   * @param contentType
   * @param code
   * @return
   *
   * For Example:
   * home-(movie|tv|zongyi|comic|jilu|mv|xiqu)-(\\w+)
   * val thirdPathName = getPathNameByCode(x.group(1),x.group(2))
   */
  def getPathNameByCode(contentType:String,code:String)={
    if(code_path_name_map.size == 0){
      val url = "http://vod.tvmore.com.cn/Service/ProgramSite_new.jsp"
      getCodeToNameMapUtil(url)
    }
    val codeToName = code_path_name_map.getOrElse(contentType,null)
    var result = code
    if (codeToName != null && codeToName.size > 0){
      result = codeToName.getOrElse(code,code)
    }
    result
  }

  /**
   *
   * @param contentType
   * @param code
   * @return
   *
   * For Example:
   * home-kids-(\\w+)-(\\w+)
   * val thirdPathName = getPathNameByCode(x.group(1),x.group(1))
   * val fourPathName = getPathNameByCode(x.group(1),x.group(2))
   */
  def getKidsPathNameByCode(contentType:String,code:String)={
    if(kids_code_path_name_map.size == 0){
      val url_kids = "http://vod.tvmore.com.cn/Service/ProgramSite_kids.jsp"
      getCodeToNameMapByKidsUtil(url_kids)
    }
    val codeToName = kids_code_path_name_map.getOrElse(contentType,null)
    var result = code
    if (codeToName != null && codeToName.size > 0){
      result = codeToName.getOrElse(code,code)
    }
    result
  }


  /**
   *
   * @param contentType
   * @param code
   * @return
   *
   * For Example:
   * home-sports-(\\w+)-(\\w+)
   * val thirdPathName = getPathNameByCode(x.group(1),x.group(1))
   * val fourPathName = getPathNameByCode(x.group(1),x.group(2))
   */
  def getSportPathNameByCode(contentType:String,code:String)={
    if(sport_code_path_name_map.size == 0){
      val url_sport = "http://vod.tvmore.com.cn/Service/V2/ProgramSite_Sport.jsp"
      getCodeToNameMapBySportUtil(url_sport)
    }
    val codeToName = sport_code_path_name_map.getOrElse(contentType,null)
    var result = code
    if (codeToName != null && codeToName.size > 0){
      result = codeToName.getOrElse(code,code)
    }
    result
  }

  /**
   *
   * @param contentType
   * @param code1
   * @param code2
   * @return
   *
   * For Example:
   * home-sports-(\\w+)-(\\w+)-(\\w+)
   * val fivePathName = getPathNameByCode(x.group(1),x.group(2),x.group(3))
   */
  def getSportPathNameByCode(contentType:String,code1:String,code2:String)={
    if(sport_code_four_path_name_map.size == 0){
      val url_sport = "http://vod.tvmore.com.cn/Service/V2/ProgramSite_Sport.jsp"
      getCodeToNameMapBySportUtil(url_sport)
    }
    val codeToName = sport_code_four_path_name_map.getOrElse(contentType,null)
    var result = code2
    if (codeToName != null && codeToName.size > 0){
      val temp = codeToName.getOrElse(code1,null)
      if(temp != null && temp.size > 0){
        result = temp.getOrElse(code2,code2)
      }
    }
    result
  }


  /** "========================================some utils=====================================" */

  /**
   * translate JsonObject to Map
   */
  def getCodeToNameMapUtil(url:String)={
    val json = HttpUtils.get(url)
    val jsonObj = new JSONObject(json)
    val jsonArray = jsonObj.getJSONArray("children")
    for(i <- 0 until jsonArray.length()){
      val arr:JSONObject = jsonArray.getJSONObject(i)
      val contentType = arr.getString("contentType")
      val children = arr.getJSONArray("children")
      val child_code_name_map:Map[String,String] = Map()
      for(j <- 0 until children.length()){
        val child:JSONObject = children.getJSONObject(j)
        child_code_name_map += (child.getString("code") -> child.getString("name"))
      }
      if(contentType == "movie" || contentType == "tv") child_code_name_map += ("search" -> "搜索")
      child_code_name_map += ("retrieval" -> "筛选")
      code_path_name_map += (contentType -> child_code_name_map)
    }
    code_path_name_map
  }

  /**
   * translate JsonObject to Map which contentType is kids
   */
  def getCodeToNameMapByKidsUtil(url:String)={
    val json = HttpUtils.get(url)
    val jsonObj = new JSONObject(json)
    val jsonArray = jsonObj.getJSONArray("children")
    for(i <- 0 until jsonArray.length()){
      val arr:JSONObject = jsonArray.getJSONObject(i)
      val contentType = arr.getString("code")
      val name = arr.getString("name")
      val children = arr.getJSONArray("children")
      val child_code_name_map:Map[String,String] = Map()
      child_code_name_map += (contentType -> name)
      if (children != null && children.length() > 0) {
        for(j <- 0 until children.length()){
          val child:JSONObject = children.getJSONObject(j)
          child_code_name_map += (child.getString("code") -> child.getString("name"))
        }
      }
      kids_code_path_name_map += (contentType -> child_code_name_map)
    }
    kids_code_path_name_map
  }

  /**
   * translate JsonObject to Map which contentType is kids
   */
  def getCodeToNameMapBySportUtil(url:String)={
    val json = HttpUtils.get(url)
    val jsonObj = new JSONObject(json)
    val jsonArray = jsonObj.getJSONArray("children")
    for(i <- 0 until jsonArray.length()){
      val arr:JSONObject = jsonArray.getJSONObject(i)
      val contentType = arr.getString("code")
      val name = arr.getString("name")
      val children = arr.getJSONArray("children")

      val child_code_name_map:Map[String,String] = Map()
      val four_child_code_name_map:Map[String,Map[String,String]] = Map()

      child_code_name_map += (contentType -> name)
      if (children != null && children.length() > 0) {
        for(j <- 0 until children.length()){
          val child:JSONObject = children.getJSONObject(j)
          child_code_name_map += (child.getString("code") -> child.getString("name"))
          //判断是否有四级目录
          val four_child = child.getJSONArray("children")
          if(four_child != null && four_child.length() > 0){
            val four_path_code_name_map:Map[String,String] = Map()
            for(k <- 0 until four_child.length()){
              val temp = four_child.getJSONObject(j)
              four_path_code_name_map += (temp.getString("code") -> temp.getString("name"))
            }
            four_child_code_name_map += (child.getString("code") -> four_path_code_name_map)
          }
        }
      }
      kids_code_path_name_map += (contentType -> child_code_name_map)
      sport_code_four_path_name_map += (contentType -> four_child_code_name_map)
    }
  }
}
