package com.moretv.bi.util

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import org.json.JSONObject
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}

/**
 * Created by xia jun  on 2016-08-24.
  * 连接redis，通过sid查询节目对应的duration
 */
object ProgramDurationUtil {

  /**
   * 定义ProgramRedis使用的一些常量
   */
  val TITLE = "item_title"
  val AREA = "item_area"
  val DURATION = "item_duration"
  val CONTENT_TYPE = "item_contentType"
  val SUPPLY_TYPE = "supply_type"

  /**
   * 初始化Jedis配置
   */
  val config:JedisPoolConfig = new JedisPoolConfig()
  var metadataPool:JedisPool = null
  val db = DataIO.getRedisOps(DataBases.REDIS_3_3)
  val metadata_host=db.prop.getProperty("metadata_host")
  val metadata_port= db.prop.getProperty("metadata_port").toInt
  val metadata_db= db.prop.getProperty("metadata_db").toInt

  /**
   * 初始化Jedis对象
   */
  def init()={
    config.setMaxTotal(10)
    config.setMaxWaitMillis(10000)
    metadataPool = new JedisPool(config ,metadata_host, metadata_port,100* Protocol.DEFAULT_TIMEOUT,null,metadata_db)
  }

  /**
   * 从redis库中将sid转换为title
   */
  def getTitleBySid(sid:String)={
    if(metadataPool == null){
      init()
    }
    try {
      val metadata_jedis = metadataPool.getResource
      val metadata = metadata_jedis.get(sid.toString)
      var title = sid
      if (metadata != null && metadata != "nil") {
        val jsonObject = new JSONObject(metadata)
        title = jsonObject.getJSONArray(TITLE).get(0).toString()
        if (title != null) {
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
      case e:Exception => sid
    }
  }


  /**
   * 从redis数据库中获取节目的时长
   * @param sid
   */
  def getDurationBySidFromRedis(sid:String) = {
    var duration = ""
    if(metadataPool == null){
      init()
    }
    try {
      val metadata_jedis = metadataPool.getResource
//      val id = CodeIDOperator.codeToId(sid)
      val metadata = metadata_jedis.get(sid.toString)
      if (metadata != null && metadata != "nil") {
        val jsonObject = new JSONObject(metadata)
        duration = jsonObject.getJSONArray(DURATION).get(0).toString
      }
      metadataPool.returnResource(metadata_jedis)
      duration
    } catch {
      case e:Exception => duration
    }
  }


  /**
   * close jedis
   */
  def destroy(){
    metadataPool.destroy()
  }

}
