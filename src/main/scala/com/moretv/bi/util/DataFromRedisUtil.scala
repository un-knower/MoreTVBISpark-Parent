package com.moretv.bi.util

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}
import org.json.JSONObject
/**
 * Created by xia jun on 2016/9/4.
 * 该类用于从redis数据库中提取相关信息
 */
object DataFromRedisUtil {

  val TITLE = "title"
  val DISPLAY_NAME = "display_name"
  /**
   * 设置redis数据库相关的变量
   */
  val config:JedisPoolConfig = new JedisPoolConfig()       //创建redis的配置对象
  var metadataPool:JedisPool = null                        //创建redis的数据池
  val db0 = DataIO.getRedisOps(DataBases.REDIS_3_0)
  val db1 = DataIO.getRedisOps(DataBases.REDIS_3_1)
  val db2 = DataIO.getRedisOps(DataBases.REDIS_3_2)
  val db3 = DataIO.getRedisOps(DataBases.REDIS_3_3)
  val metadata_host=db0.prop.getProperty("metadata_host")                            //创建redis的host IP信息
  val metadata_port= db0.prop.getProperty("metadata_port").toInt                                //创建redis的端口号信息
  val mtv_subject_metadata_db=db0.prop.getProperty("metadata_db").toInt                            //创建redis中的数据库
  val mtv_channel_metadata_db = db1.prop.getProperty("metadata_db").toInt                          //创建redis中的数据库
  val sailfish_sport_match_metadata_db = db2.prop.getProperty("metadata_db").toInt                 //创建redis中的数据库
  val mtv_basecontent_metadata_db =db3.prop.getProperty("metadata_db").toInt                       //创建redis中的数据库


  /**
   * 初始化redis数据库的连接信息
   * @param metadata_db
   */
  def init(metadata_db:Int) = {
    config.setMaxTotal(100)
    config.setMaxWaitMillis(10000)
    metadataPool = new JedisPool(config,metadata_host,metadata_port,100*Protocol.DEFAULT_TIMEOUT,null,metadata_db)
  }

  /**
   * 根据直播频道的sid，提取直播频道的名称
   * @param sid
   */
  def getLiveChannelNameFromSid(sid:String) = {
    if(metadataPool == null){
      init(mtv_channel_metadata_db)
    }
    try {
      val metadata_jedis = metadataPool.getResource
      val metadata = metadata_jedis.get(sid)
      var title = sid
      if(metadata != null && metadata != "nil"){
        val jsonObj = new JSONObject(metadata)
        title = jsonObj.optString(TITLE)
      }
      metadataPool.returnResource(metadata_jedis)
      title
    }catch {
      case e:Exception => {
        sid
      }
    }
  }


  /**
   * 根据频道的sid，提取体育直播频道的名称
   * @param sid
   */
  def getSportLiveNameFromSid(sid:String) = {
    if(metadataPool != null){
      init(sailfish_sport_match_metadata_db)
    }
    try {
      val metadata_jedis = metadataPool.getResource
      val metadata = metadata_jedis.get(sid)
      var title = sid
      if(metadata != null && metadata != "nil"){
        val jsonObj = new JSONObject(metadata)
        title = jsonObj.optString(TITLE)
      }
      metadataPool.returnResource(metadata_jedis)
      title
    }catch {
      case e:Exception => {
        sid
      }
    }
  }

  /**
   * 根据专题code转换，提取专题名称
   * @param subjectCode
   */
  def getSubjectTitleFromSubjectCode(subjectCode:String) = {
    if(metadataPool != null){
      init(sailfish_sport_match_metadata_db)
    }
    try {
      val metadata_jedis = metadataPool.getResource
      val metadata = metadata_jedis.get(subjectCode)
      var title = subjectCode
      if(metadata != null && metadata != "nil"){
        val jsonObj = new JSONObject(metadata)
        title = jsonObj.optString(TITLE)
      }
      metadataPool.returnResource(metadata_jedis)
      title
    }catch {
      case e:Exception => {
        subjectCode
      }
    }
  }

  /**
   * 获取节目的title
   * @param sid
   * @return
   */
  def getProgramTitleBySid(sid:String)= {
    if (metadataPool == null) {
      init(mtv_basecontent_metadata_db)
    }
    try {
      val metadata_jedis = metadataPool.getResource
      val metadata = metadata_jedis.get(sid)
      var title = sid
      if (metadata != null && metadata != "nil") {
        val jsonObject = new JSONObject(metadata)
        title = jsonObject.optString(DISPLAY_NAME)
          if (title != "") {
            title = title.replace("'", "")
            title = title.replace("\t", " ")
            title = title.replace("\r", "-")
            title = title.replace("\n", "-")
            title = title.replace("\r\n", "-")
        } else title = sid
      }
      metadataPool.returnResource(metadata_jedis)
      title
    } catch {
      case e: Exception => sid
    }
  }

}
