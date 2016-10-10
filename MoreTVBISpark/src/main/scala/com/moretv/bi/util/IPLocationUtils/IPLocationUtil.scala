package com.moretv.bi.util.IPLocationUtils

import java.util

import com.moretv.bi.util.HdfsUtil

import scala.io.Source

/**
 * Created by xia jun on 2016/7/22.
 * For IP map
 */
object IPLocationDataUtil {
  val ipLocationMap = new util.HashMap[String, Array[String]]()
  if (ipLocationMap.isEmpty) {
    load()
  }

  def load() {
    val inputStream = HdfsUtil.getHDFSFileStream("/log/ipLocationData/ip_country.txt")
    val lines = Source.fromInputStream(inputStream).getLines()
    lines.foreach(line => {
      val lineSplit = line.split("\t")
      // 构建以IP前三段为key，剩余信息为value的Map
      val key = getFirst3IP(lineSplit(0))
      val value = lineSplit.drop(2)
      ipLocationMap.put(key, value)
    })
  }

  def getFirst3IP(ip: String) = {
    val ipInfo = ip.split("\\.")
    if(ipInfo.length >= 3){
      ipInfo(0).concat("*").concat(ipInfo(1)).concat("*").concat(ipInfo(2))
    } else ip
  }

  def getCountry(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val country = value(0)
        country
      } else "未知"
    } else "未知"

  }

  def getProvince(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val province = value(1)
        province
      } else "未知"
    } else "未知"
  }

  def getCity(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val city = value(2)
        city
      } else "未知"
    } else "未知"
  }

  def getDistrict(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val district = value(1)
        district
      } else "未知"
    } else "未知"
  }
}
