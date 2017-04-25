package com.moretv.bi.util.IPLocationUtils

import java.util

import com.moretv.bi.constant.Constants
import com.moretv.bi.util.{HdfsUtil, SparkSetting}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

/**
 * Created by xia jun on 2016/7/22.
 * For IP 运营商
 */
@deprecated
object IPOperatorsUtil extends SparkSetting{
  private val IPISPMap = new mutable.HashMap[String,Array[String]]()
  private val IPArrayBuffer = new ArrayBuffer[String]()
  private val IPArr  = load()

  def load() = {
    val inputStream = HdfsUtil.getHDFSFileStream(Constants.IP_ISP_PATH)
    val lines = Source.fromInputStream(inputStream).getLines()
    lines.foreach(line=>{
      val lineSplit = line.split("\t")
      val startIp = lineSplit(0)
      val endIp = lineSplit(1)
      val value = lineSplit.drop(2)
      IPISPMap += startIp -> value
      IPISPMap += endIp -> value
      IPArrayBuffer.append(startIp)
      IPArrayBuffer.append(endIp)
    })
    IPArrayBuffer.toArray
  }

  /**
   * 获取ISP信息
   * @param ip
   * @return
   */
  def getISPInfo(ip:String):String = {
    var result = "未知"
    val ipNew = completeIp(ip)
    val keys = binarySearch(IPArr,ipNew)
    val value1 = IPISPMap.getOrElse(keys(0),Array("未知","未知","未知","未知","未知","未知"))
    val value2 = IPISPMap.getOrElse(keys(1),Array("未知","未知","未知","未知","未知","未知"))
    if(value1.sameElements(value2)){
      result = value1(4)
    }
    result
  }

  /**
   * 获取国家信息
   * @param ip
   * @return
   */
  def getCountry(ip:String):String = {
    val ipNew = completeIp(ip)
    var result = "未知"
    val keys = binarySearch(IPArr,ipNew)
    val value1 = IPISPMap.getOrElse(keys(0),Array("未知","未知","未知","未知","未知","未知"))
    val value2 = IPISPMap.getOrElse(keys(1),Array("未知","未知","未知","未知","未知","未知"))
    if(value1.sameElements(value2)){
      result = value1(0)
    }
    result
  }

  /**
   * 获取省份信息
   * @param ip
   * @return
   */
  def getProvince(ip:String):String = {
    val ipNew = completeIp(ip)
    var result = "未知"
    val keys = binarySearch(IPArr,ipNew)
    val value1 = IPISPMap.getOrElse(keys(0),Array("未知","未知","未知","未知","未知","未知"))
    val value2 = IPISPMap.getOrElse(keys(1),Array("未知","未知","未知","未知","未知","未知"))
    if(value1.sameElements(value2)){
      result = value1(1)
    }
    result
  }

  /**
   * 获取城市信息
   * @param ip
   * @return
   */
  def getCity(ip:String):String = {
    val ipNew = completeIp(ip)
    var result = "未知"
    val keys = binarySearch(IPArr,ipNew)
    val value1 = IPISPMap.getOrElse(keys(0),Array("未知","未知","未知","未知","未知","未知"))
    val value2 = IPISPMap.getOrElse(keys(1),Array("未知","未知","未知","未知","未知","未知"))
    if(value1.sameElements(value2)){
      result = value1(2)
    }
    result
  }

  /**
   * 类二分查找
   * @param iPArray
   * @param ip
   * @return
   */
  def binarySearch(iPArray: Array[String],ip:String):Array[String] = {
    var low = 0
    var high = iPArray.length-1
    var result = Array(" "," ")
    if(iPArray(low)==ip){
      result = Array(iPArray(low),iPArray(low))
    }else if(iPArray(high)==ip){
      result = Array(iPArray(high),iPArray(high))
    }else if(iPArray(low) < ip && iPArray(high) > ip){
      while (low<=high){
        val middle = (low+high)/2
        if(high-low<=1){
          result = Array(iPArray(low),iPArray(high))
          low = high +100
        }
        if(ip<iPArray(middle)){
          if(ip>iPArray(middle-1)){
            result = Array(iPArray(middle-1),iPArray(middle))
            low = high +100
          }else {
            high = middle - 1
          }
        }else if(ip>iPArray(middle)){
          if(ip<iPArray(middle+1)){
            result = Array(iPArray(middle),iPArray(middle+1))
            low = high +100
          }else{
            low = middle + 1
          }
        }else{
          result = Array(iPArray(middle),iPArray(middle))
          low = high +100
        }
      }
    }
    result
  }

  /**
   * 补全IP
   * @param ip
   * @return
   */
  def completeIp(ip:String) = {
    val ipSplit = ip.split("\\.")
    val ipNewList = new ListBuffer[String]()
    ipSplit.foreach(e=>{
      e.length match {
        case 1 => ipNewList.append(s"00${e}")
        case 2 => ipNewList.append(s"0${e}")
        case _ => ipNewList.append(e)
      }
    })
    ipNewList.toArray.mkString(".")
  }

  /**
    * 获取真实IP，取forwardedIp的第一个IP
    *
    * @param forwardedIp
    * @param remoteIp
    * @return
    */
  def getIPInfo(forwardedIp: String, remoteIp: String) = {
    if (forwardedIp != null & forwardedIp != "") {
      if (forwardedIp.contains(",")) {

        forwardedIp.split(",")(0)
      } else forwardedIp
    } else {
      remoteIp
    }
  }
}
