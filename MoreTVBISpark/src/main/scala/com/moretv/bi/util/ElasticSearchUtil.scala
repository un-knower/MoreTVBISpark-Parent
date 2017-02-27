package cn.whaley.bi.utils

import java.net.InetAddress
import java.util

import com.moretv.bi.constant.Constants
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConversions._

/**
  * Created by laishun on 16/6/22.
  */
object ElasticSearchUtil {
  private var client: Client = null

  //初始化client
  def init = {
    val settings = Settings.settingsBuilder().put("cluster.name", "monitor").build()
    client = TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Constants.HZ_9_HOST), Constants.HZ_ES_PORT))
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Constants.HZ_8_HOST), Constants.HZ_ES_PORT))
  }

  //关闭cient
  def close = {
    if (client != null)
      client.close()
  }

  /**
    *
    * @param json     存储内容
    * @param index    索引名称
    * @param typeName 类型名称
    * @param id       标示id
    * @return
    */
  def createIndex(json: String, index: String, typeName: String, id: String = "") = {
    if (client == null) init
    if (id == "")
      client.prepareIndex(index, typeName).setSource(json).get()
    else
      client.prepareIndex(index, typeName, id).setSource(json).get()
  }

  /**
    * 批量增加
    *
    * @param list
    * @param index
    * @param typeName
    */
  def bulkCreateIndex(list: util.ArrayList[util.Map[String, Object]], index: String, typeName: String) = {
    try {
      if (client == null) init
      val bulkRequest = client.prepareBulk()
      list.foreach(x => {
        val xb = XContentFactory.jsonBuilder().startObject()
        x.foreach(e => {
          xb.field(e._1, if (e._2.isInstanceOf[Long]) {
            e._2.toString.toLong
          } else if (e._2.isInstanceOf[String]) {
            e._2.toString
          })
        })
        xb.endObject()
        bulkRequest.add(client.prepareIndex(index, typeName).setSource(xb))
      })
      bulkRequest.get()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def bulkCreateIndex1(list: util.ArrayList[Map[String, Object]], index: String, typeName: String) = {
    try {
      if (client == null) init
      val bulkRequest = client.prepareBulk()
      list.foreach(x => {
        val xb = XContentFactory.jsonBuilder().startObject()
        x.foreach(e => {
          xb.field(e._1, if (e._2.isInstanceOf[Long]) {
            e._2.toString.toLong
          } else if (e._2.isInstanceOf[String]) {
            e._2.toString
          })
        })
        xb.endObject()
        bulkRequest.add(client.prepareIndex(index, typeName).setSource(xb))
      })
      bulkRequest.get()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }



  /**
    *
    * @param index
    * @param typeName
    * @param id
    */
  def getIndex(index: String, typeName: String, id: String) = {
    if (client == null) init
    client.prepareGet(index, typeName, id).get()
  }

  /**
    *
    * @param index
    * @param typeName
    * @param id
    */
  def deleteIndex(index: String, typeName: String, id: String) = {
    if (client == null) init
    client.prepareDelete(index, typeName, id).get()
  }


  def searchIndex(index: String, typeName: String, sid: String, startDate: String, endDate: String): Unit = {
    if (client == null) init
    val searchResponce = client.prepareSearch(index).setTypes(typeName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setQuery(QueryBuilders.matchQuery("sid", sid))
      .setPostFilter(QueryBuilders.rangeQuery("day").from(startDate).to(endDate))
      .execute()
      .actionGet()
    val hits = searchResponce.getHits
    if (hits != null) {
      val hitArray = hits.getHits
      for (i <- 0 until hitArray.length) {
        val hit = hitArray(i)
        //val fieldMap = hit.getSource
        println(hit.getSourceAsString)
      }
    }
  }
}
