
package com.moretv.bi.util

import moretv.openapi.service.IIpService
import moretv.openapi.service.impl.IpServiceImpl2

@deprecated
object IPUtils {
    private val instance:IIpService = new IpServiceImpl2()

    def getIpLocationByIp(ip:String) = {
        instance.getLocationByIp(ip)
    }

    def getProvinceByIp(ip:String) = {
        val ipLocation = getIpLocationByIp(ip)
        val str = if(ipLocation != null) {
            ipLocation.getAreaStr.substring(0, 2)
        } else "未知"

        provinceFix(str)
    }

    def getProvinceAndCityByIp(ip:String) = {
        val ipLocation = getIpLocationByIp(ip)
        if(ipLocation != null) {
            val area = ipLocation.getAreaStr
            if(area != null && !area.isEmpty){
                val province = provinceFix(area.substring(0, 2))
                val idx = area.indexOf("市")
                if(idx > 0){
                    val city = area.substring(0,idx+1)
                    Array(province,city)
                }else Array(province,area)
            }else null
        } else null
    }

    def getIspByIp(ip:String) = {
        val ipLocation = getIpLocationByIp(ip)
        if(ipLocation != null) {
            ipLocation.getIsp
        } else "未知"
    }

    def getWholeLocationByIp(ip:String) = {
        val ipLocation = getIpLocationByIp(ip)
        val str = if(ipLocation != null) {
            ipLocation.getAreaStr
        } else "未知"
        provinceFix(str)
    }

    def provinceFix(province:String) = {
        if(province == null){
            "未知"
        }else if(province == "黑龙"){
            "黑龙江"
        }else if(province == "内蒙"){
            "内蒙古"
        }else province
    }

}

