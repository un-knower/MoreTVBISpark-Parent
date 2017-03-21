package com.moretv.bi.report.medusa

import com.moretv.bi.util.CodeIDOperator
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}

/**
  * Created by xiajun on 2017/3/20.
  */
object RedisTest3 extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
//    println("********************************************")
//    println(ProgramRedisUtil.getTitleBySid("1cu96knpp8k7"))
//    println(ProgramRedisUtil.getTitleBySid("5ifhwya1fhm7"))
    println("********************************************")

        println(getTitle("1cu96knpp8k7"))
        println(getTitle("5ifhwya1fhm7"))

//    val test = """\xe4\xb8\xad\xe5\x9b\xbd"""
//    val newStr = new String(test.getBytes("unicode"),"UTF-8")
//    println(newStr)
  }

  def getTitle(sid:String) = {
    val host = "10.255.130.6"
    val port = 6379
    val db = 3

    val config:JedisPoolConfig = new JedisPoolConfig()
    var metadataPool:JedisPool = null


    config.setMaxTotal(10)
    config.setMaxWaitMillis(10000)
    metadataPool = new JedisPool(config ,host, port,100* Protocol.DEFAULT_TIMEOUT,null,db)
    try {
      val metadata_jedis = metadataPool.getResource
      val metadata = metadata_jedis.get(sid)
      var title = sid
      if (metadata != null && metadata != "nil") {
        val jsonObject = new JSONObject(metadata)
        title = jsonObject.getString("display_name")
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
      case e:Exception => {
        e.printStackTrace()
        sid
      }
    }
  }

}
