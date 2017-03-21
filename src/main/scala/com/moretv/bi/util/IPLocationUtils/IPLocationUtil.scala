package com.moretv.bi.util.IPLocationUtils

import java.util

import com.moretv.bi.constant.Constants
import com.moretv.bi.util.HdfsUtil

import scala.io.Source

/**
  * Created by xia jun on 2016/7/22.
  * For IP map
  */
@Deprecated
object IPLocationDataUtil {
  val ipLocationMap = new util.HashMap[String, Array[String]]()
  if (ipLocationMap.isEmpty) {
    load()
  }

  def load() {
    val inputStream = HdfsUtil.getHDFSFileStream(Constants.IP_AREA_PATH)
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
    if (ipInfo.length >= 3) {
      ipInfo(0).concat("*").concat(ipInfo(1)).concat("*").concat(ipInfo(2))
    } else ip
  }

  /**
   * 获取国家信息
   * @param ip
   * @return
   */
  def getCountry(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val country = value(0)
        country
      } else IPOperatorsUtil.getCountry(ip)
    } else "未知"

  }

  /**
   * 获取省份信息
   * @param ip
   * @return
   */
  def getProvince(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val province = value(1)
        province
      } else IPOperatorsUtil.getProvince(ip)
    } else "未知"
  }

  /**
   * 获取城市信息
   * @param ip
   * @return
   */
  def getCity(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val city = value(2)
        city
      } else IPOperatorsUtil.getCity(ip)
    } else "未知"
  }

  /**
   * 获取区县信息
   * @param ip
   * @return
   */
  def getDistrict(ip: String) = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        val district = value(3)
        district
      } else "未知"
    } else "未知"
  }

  def getProvinceCity(ip: String): String = {
    if (ip != null) {
      val value = ipLocationMap.getOrDefault(getFirst3IP(ip), null)
      if (value != null) {
        s"${value(1)}-${value(2)}"
      }
      else s"${IPOperatorsUtil.getProvince(ip)}-${IPOperatorsUtil.getCity(ip)}"
    } else "未知-未知"

  }


}
