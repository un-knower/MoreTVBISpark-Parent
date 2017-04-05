package com.moretv.bi.util

/**
  * Created by Will on 2016/6/4.
  */
@deprecated
object ProvinceUtil {

  val provinces = List("上海", "云南", "内蒙古", "北京", "台湾", "吉林", "四川",
    "天津", "宁夏", "安徽", "山西", "山东", "广东", "广西", "新疆", "江苏", "江西", "河北",
    "河南", "浙江", "海南", "湖北", "湖南", "澳门", "甘肃", "福建", "贵州", "辽宁", "重庆",
    "青海", "香港", "黑龙江", "陕西", "西藏")

  def getChinaProvince(province:String) = {
    if(provinces.contains(province)) province else "国外及其他"
  }
}
