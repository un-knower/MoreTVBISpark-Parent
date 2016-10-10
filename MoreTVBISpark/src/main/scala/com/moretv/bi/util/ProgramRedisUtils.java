package com.moretv.bi.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.HashMap;
import java.util.Map;

public class ProgramRedisUtils {

	//定义ProgramRedis使用的一些常量
	public static final String TITLE = "item_title";
	public static final String AREA = "item_area";
	public static final String CONTENT_TYPE = "item_contentType";

	private JedisPoolConfig config = new JedisPoolConfig();
    private JedisPool metadataPool;
    private String metadata_host="10.10.2.17";
    private int metadata_port=6379;
    private int metadata_db=0;
    private Jedis metadata_jedis=null;



    public ProgramRedisUtils() {
		config.setMaxTotal(10);
		config.setMaxWaitMillis(10000);
		metadataPool=new JedisPool(config ,metadata_host,
				metadata_port,100* Protocol.DEFAULT_TIMEOUT,null,metadata_db);
		metadata_jedis=metadataPool.getResource();
	}


	public Map<String, String> getFieldValues(String sid, String... fieldNames) throws JSONException {
		String metadata=null;
		Integer id = null;
		try {
			id = CodeIDOperator.codeToId(sid);
		}catch (Exception e){
			System.out.println("Exception:" + e.getMessage() + "\t" +  e.getClass());
			System.out.println("sid:" + sid);
		}
		if(id == null) return null;
		metadata=metadata_jedis.get(id.toString());
		if(metadata==null || "nil".equals(metadata)) return null;

		JSONObject jsonObject =new JSONObject(metadata);
		Map<String, String> map = new HashMap<String, String>();
		for (String fieldName : fieldNames) {
			JSONArray jsonArray = jsonObject.getJSONArray(fieldName);
			String fieldValue = null;
			if(jsonArray != null && jsonArray.length() > 0){
				fieldValue=jsonArray.get(0).toString();
			}
			map.put(fieldName, fieldValue);
		}
		return map;
	}

    public String getTitleBySid(String sid) throws JSONException {
    	String metadata=null;
		Integer id = null;
		try {
			id = CodeIDOperator.codeToId(sid);
		}catch (Exception e){
			System.out.println("Exception:" + e.getMessage() + "\t" +  e.getClass());
			System.out.println("sid:" + sid);
		}
		if(id == null) return null;
    	metadata=metadata_jedis.get(id.toString());
	    if(metadata==null || "nil".equals(metadata)) return null;

	    JSONObject jsonObject =new JSONObject(metadata);
		String title=jsonObject.getJSONArray(ProgramRedisUtils.TITLE).get(0).toString();

		if (title != null) {
			title = title.replace("'", "");
			title = title.replace("\t", " ");
			title = title.replace("\r", "-");
			title = title.replace("\n", "-");
			title = title.replace("\r\n", "-");
		}else title = sid;

		return title;
    }

	public String getAeraBySid(String sid) throws JSONException {
    	String metadata=null;
		Integer id = null;
		try {
			id = CodeIDOperator.codeToId(sid);
		}catch (Exception e){
			System.out.println("Exception:" + e.getMessage() + "\t" +  e.getClass());
			System.out.println("sid:" + sid);
		}
		if(id == null) return null;
    	metadata=metadata_jedis.get(id.toString());
	    if(metadata==null || "nil".equals(metadata)) return null;

	    JSONObject jsonObject =new JSONObject(metadata);
		String area=jsonObject.getJSONArray(ProgramRedisUtils.AREA).get(0).toString();
		return area;
    }

    public void destroy(){
    	metadataPool.returnResource(metadata_jedis);
    	metadataPool.destroy();
    }
}
