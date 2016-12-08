package com.moretv.bi.util

import org.json.JSONObject
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}

/**
 * Created by 连凯 on 2016-07-04.
  * 连接redis，通过sid查询节目对应的title
 */
object ProgramRedisUtil {

  /**
   * 定义ProgramRedis使用的一些常量
   */
  val TITLE = "item_title"
  val AREA = "item_area"
  val CONTENT_TYPE = "item_contentType"
  val SUPPLY_TYPE = "supply_type"

  /**
   * 初始化Jedis配置
   */
  val config:JedisPoolConfig = new JedisPoolConfig()
  var metadataPool:JedisPool = null
  val metadata_host="10.10.2.17"
  val metadata_port= 6379
  val metadata_db=0

  /**
   * 初始化Jedis对象
   */
  def init()={
    config.setMaxTotal(10)
    config.setMaxWaitMillis(10000)
    metadataPool = new JedisPool(config ,metadata_host, metadata_port,100* Protocol.DEFAULT_TIMEOUT,null,metadata_db)
  }

  /**
   * Function:get program title by program sid
   * @param sid
   * @return
   */
  def getTitleBySid(sid:String)={
    if(metadataPool == null){
      init()
    }
    try {
      val metadata_jedis = metadataPool.getResource
      val id = CodeIDOperator.codeToId(sid)
      val metadata = metadata_jedis.get(id.toString)
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
   * close jedis
   */
  def destroy(){
    metadataPool.destroy()
  }

}
