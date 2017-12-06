package com.moretv.bi.util

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.alibaba.fastjson.{JSONObject,JSON}
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}

/**
 * Created by 连凯 on 2016-07-04.
  * 连接redis，通过sid查询节目对应的title
 */
object ProgramRedisUtil {

  /**
   * 定义ProgramRedis使用的一些常量
   */
  val TITLE = "display_name"
  val CONTENT_TYPE = "content_type"

  /**
   * 初始化Jedis配置
   */
  val config:JedisPoolConfig = new JedisPoolConfig()
  var metadataPool:JedisPool = null
  private val db = DataIO.getRedisOps(DataBases.REDIS_17_0)
  private val metadata_host=db.prop.getProperty("metadata_host")
  private val metadata_port=db.prop.getProperty("metadata_port").toInt
  private val metadata_db=3

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
    val metadata_jedis = metadataPool.getResource
    try {
      val metadata = metadata_jedis.get(sid)
      var title = sid
      if (metadata != null && metadata != "nil") {
        val jsonObject =  JSON.parseObject(metadata)
        title = jsonObject.getString(TITLE)

        if (title != null) {
          title = title.replace("'", "")
          title = title.replace("\t", " ")
          title = title.replace("\r", "-")
          title = title.replace("\n", "-")
          title = title.replace("\r\n", "-")
        } else title = sid
      }
      title
    } catch {
      case e:Exception => sid
    }finally {
      metadataPool.returnResource(metadata_jedis)
    }
  }


  /**
   * close jedis
   */
  def destroy(){
    metadataPool.destroy()
  }

}
