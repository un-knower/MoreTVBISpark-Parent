package com.moretv.bi.util

import java.util.{HashMap, Map}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Protocol}

class ProgramRedisUtils {

	//定义ProgramRedis使用的一些常量
	val TITLE = "item_title"
	val AREA = "item_area"

	private val config = new JedisPoolConfig()
    private var metadataPool:JedisPool = null
    private val metadata_host="10.10.2.17"
    private val metadata_port=6379
    private val metadata_db=0
    private var metadata_jedis:Jedis=null

		config.setMaxTotal(10)
		config.setMaxWaitMillis(10000)
		metadataPool=new JedisPool(config ,metadata_host,
			metadata_port,100* Protocol.DEFAULT_TIMEOUT,null,metadata_db)
		metadata_jedis=metadataPool.getResource()


	def  getFieldValues(sid:String, fieldNames:String *):Map[String, String] = {
		var metadata:String=null
		var id:Integer = null
		try {
			id = CodeIDOperator.codeToId(sid)
		}catch {
			case e:Exception => {
				System.out.println("Exception:" + e.getMessage() + "\t" +  e.getClass())
				System.out.println("sid:" + sid)
			}
			case _ =>
		}
		if(id != null){
			metadata=metadata_jedis.get(id.toString())
			if(metadata==null || "nil".equals(metadata)) null else {
				val jsonObject =new JSONObject(metadata)
				val map:Map[String, String] = new HashMap[String, String]()
				fieldNames.foreach(filedName => {
					val jsonArray = jsonObject.getJSONArray(filedName.toString)
					var fieldValue:String = null
					if(jsonArray != null && jsonArray.length() > 0){
						fieldValue=jsonArray.get(0).toString()
					}
					map.put(filedName.toString, fieldValue)
				})
				map
			}
		} else null
	}

	def getTitleBySid(sid:String) = {
		try {
			val id = CodeIDOperator.codeToId(sid)
			if(id != null) {
				val metadata = metadata_jedis.get(id.toString)
				if (metadata == null || "nil" == metadata) null
				else {
					val jsonObject = new JSONObject(metadata)
					var title = jsonObject.getJSONArray(ProgramRedisUtil.TITLE).get(0).toString
					if (title != null) {
						title = title.replace("'", "")
						title = title.replace("\t", " ")
						title = title.replace("\r", "-")
						title = title.replace("\n", "-")
						title = title.replace("\r\n", "-")
					} else title = sid
					title
				}
			}else null
		}catch {
			case e:Exception => null
		}
	}

	def getAeraBySid(sid:String) = {
		var metadata:String=null
		var id:Integer = null
		try {
			id = CodeIDOperator.codeToId(sid)
		}catch {
			case e:Exception => {
				System.out.println("Exception:" + e.getMessage() + "\t" +  e.getClass())
				System.out.println("sid:" + sid)
			}
				case _ =>
		}
		if(id != null){
			metadata=metadata_jedis.get(id.toString())
			if(metadata==null || "nil".equals(metadata)) null else{
				val jsonObject =new JSONObject(metadata)
				val area=jsonObject.getJSONArray(ProgramRedisUtil.AREA).get(0).toString()
				area
			}
		} else null
	}

	def destroy() = {
		metadataPool.returnResource(metadata_jedis)
		metadataPool.destroy()
	}

}
