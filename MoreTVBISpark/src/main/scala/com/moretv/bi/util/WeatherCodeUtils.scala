package com.moretv.bi.util

import scala.collection.mutable

/**
 * Created by Will on 2015/4/18.
 */
object WeatherCodeUtils {
    private val weatherCode2ProviceMap  = new mutable.HashMap[String,String]

    def getProviceByWeatherCode(weatherCode: String): String = {
      val country: String = weatherCode.substring(0, 3)
      val provinceCode: String = if (country == "101") {
        weatherCode.substring(3, 5)
      }else "99"
//      val provinceInt = provinceCode.toInt
//      if (provinceInt < 1 || provinceInt > 34) provinceCode = "99"
      
      return weatherCode2ProviceMap.get(provinceCode).getOrElse("国外及其他地区")
    }

      weatherCode2ProviceMap += ("01" -> "北京")
      weatherCode2ProviceMap += ("02" -> "上海")
      weatherCode2ProviceMap += ("03" -> "天津")
      weatherCode2ProviceMap += ("04" -> "重庆")
      weatherCode2ProviceMap += ("05" -> "黑龙江")
      weatherCode2ProviceMap += ("06" -> "吉林")
      weatherCode2ProviceMap += ("07" -> "辽宁")
      weatherCode2ProviceMap += ("08" -> "内蒙古")
      weatherCode2ProviceMap += ("09" -> "河北")
      weatherCode2ProviceMap += ("10" -> "山西")
      weatherCode2ProviceMap += ("11" -> "陕西")
      weatherCode2ProviceMap += ("12" -> "山东")
      weatherCode2ProviceMap += ("13" -> "新疆")
      weatherCode2ProviceMap += ("14" -> "西藏")
      weatherCode2ProviceMap += ("15" -> "青海")
      weatherCode2ProviceMap += ("16" -> "甘肃")
      weatherCode2ProviceMap += ("17" -> "宁夏")
      weatherCode2ProviceMap += ("18" -> "河南")
      weatherCode2ProviceMap += ("19" -> "江苏")
      weatherCode2ProviceMap += ("20" -> "湖北")
      weatherCode2ProviceMap += ("21" -> "浙江")
      weatherCode2ProviceMap += ("22" -> "安徽")
      weatherCode2ProviceMap += ("23" -> "福建")
      weatherCode2ProviceMap += ("24" -> "江西")
      weatherCode2ProviceMap += ("25" -> "湖南")
      weatherCode2ProviceMap += ("26" -> "贵州")
      weatherCode2ProviceMap += ("27" -> "四川")
      weatherCode2ProviceMap += ("28" -> "广东")
      weatherCode2ProviceMap += ("29" -> "云南")
      weatherCode2ProviceMap += ("30" -> "广西")
      weatherCode2ProviceMap += ("31" -> "海南")
      weatherCode2ProviceMap += ("32" -> "香港")
      weatherCode2ProviceMap += ("33" -> "澳门")
      weatherCode2ProviceMap += ("34" -> "台湾")
      weatherCode2ProviceMap += ("99" -> "国外及其他地区")

}
