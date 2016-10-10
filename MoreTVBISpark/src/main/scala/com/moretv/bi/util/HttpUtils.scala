package cn.whaley.bi.utils

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
 * Created by Will on 2015/10/30.
 */
object HttpUtils {

  def get(url:String) = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    val response = httpClient.execute(httpGet)
    try{
      val entity = response.getEntity
      EntityUtils.toString(entity)
    }finally {
      response.close()
    }
  }
}
