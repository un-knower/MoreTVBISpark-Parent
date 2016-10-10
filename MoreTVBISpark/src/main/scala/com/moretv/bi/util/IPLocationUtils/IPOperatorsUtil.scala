package com.moretv.bi.util.IPLocationUtils

import java.util
import com.moretv.bi.util.{SparkSetting, HdfsUtil}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by xia jun on 2016/7/22.
 * For IP 运营商
 */
object IPOperatorsUtil extends SparkSetting{
  private val IPISPMap = new mutable.HashMap[String,Array[String]]()
  private val IPArrayBuffer = new ArrayBuffer[String]()
  private val IPArr  = load()

  def load() = {
    val inputStream = HdfsUtil.getHDFSFileStream("/log/ipLocationData/mydata4vipday2.txt")
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

  def getISPInfo(ip:String):String = {
    var result = "*"
    val keys = binarySearch(IPArr,IPISPMap,ip)
    val value1 = IPISPMap.getOrElse(keys(0),Array("*","*","*","*","*"))
    val value2 = IPISPMap.getOrElse(keys(1),Array("*","*","*","*","*"))
    if(value1.sameElements(value2)){
      result = value1(4)
    }
    result
  }

  def binarySearch(iPArray: Array[String],iPISPTreeMap:mutable.HashMap[String,Array[String]],ip:String):Array[String] = {
    var low = 0
    var high = iPArray.length-1
    var result = Array(" "," ")
    while (low<=high){
      val middle = (low+high)/2
      if(high-low<=1){
        result = Array(iPArray(low),iPArray(high))
        low = high +100
      }
      if(ip<iPArray(middle)){
        high = middle - 1
      }else if(ip>iPArray(middle)){
        low = middle + 1
      }else{
        result = Array(iPArray(low),iPArray(high))
        low = high +100
      }
    }
    result
  }
}
