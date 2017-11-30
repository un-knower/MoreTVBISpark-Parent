package cn.whaley.bi.utils

import org.apache.http.client.methods.{HttpDelete, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
 * Created by Will on 2015/10/30.
 */
object HttpUtils {

  def get(url:String,retryNum:Int) = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    val response = httpClient.execute(httpGet)
//    var retryFlag = retryNum
//    var res = ""
    try{
//      while(retryNum >= 0){
//        val entity = response.getEntity
//        val getData = EntityUtils.toString(entity)
//        if(getData == null){
//         retryFlag = retryFlag - 1
//        }else{
//          retryFlag = -1
//          res = getData
//        }
//      }
//      res
      val entity = response.getEntity
      EntityUtils.toString(entity)
    }finally {
      response.close()
    }
  }

  def delete(url:String) = {
    val httpClient = HttpClients.createDefault()
    val httpDelete = new HttpDelete(url)
    httpClient.execute(httpDelete)
  }
}
