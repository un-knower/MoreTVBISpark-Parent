package com.moretv.bi.util

import java.util.{HashMap, Map}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Database
import com.moretv.bi.global.DataBases
import org.json.JSONObject
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Protocol}

class ProgramRedisUtils {

	//定义ProgramRedis使用的一些常量
	val TITLE = "item_title"
	val AREA = "item_area"

	private val config = new JedisPoolConfig()
    private var metadataPool:JedisPool = null
		private val db = DataIO.getRedisOps(DataBases.REDIS_17_0)
    private val metadata_host=db.prop.getProperty("metadata_host")
    private val metadata_port=db.prop.getProperty("metadata_port").toInt
    private val metadata_db=6
    private var metadata_jedis:Jedis=null
	println("*****************************")
	println(s"${metadata_host} ${metadata_port}")
	println("*****************************")

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
		}
		if(id != null){
			metadata=metadata_jedis.get(id.toString())
			if(metadata==null || "nil".equals(metadata)) null else {
				val jsonObject =new JSONObject(metadata)
				val map:Map[String, String] = new HashMap[String, String]()
				fieldNames.foreach(filedName => {
					val fieldValue = jsonObject.getString(filedName.toString)
					map.put(filedName.toString, fieldValue)
				})
				map
			}
		} else null
	}

	def getTitleBySid(sid:String) = {
		try {
			val id = CodeIDOperator.codeToId(sid)
			if(id != 0) {
				val metadata = metadata_jedis.get(id.toString)
				if (metadata == null || "nil" == metadata) null
				else {
					val jsonObject = new JSONObject(metadata)
					var title = jsonObject.getString(ProgramRedisUtil.TITLE)
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

	def destroy() = {
		metadataPool.returnResource(metadata_jedis)
		metadataPool.destroy()
	}

}
