//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.moretv.bi.util;

import moretv.openapi.entity.IPLocation;
import moretv.openapi.service.IIpService;
import moretv.openapi.service.impl.IpServiceImpl2;


public class IPUtils {
    private static IIpService instance = null;

    public IPUtils() {
    }

    public static IIpService getInstance() {
        if(instance == null) {
            instance = new IpServiceImpl2();
        }

        return instance;
    }

    public static IPLocation getIpLocationByIp(String ip) throws Exception {
        return getInstance().getLocationByIp(ip);
    }

    public static String getProvinceByIp(String ip) throws Exception {
        IPLocation ipLocation = getIpLocationByIp(ip);
        String str = null;
        if(ipLocation != null) {
            str = ipLocation.getAreaStr().substring(0, 2);

        } else {
            str = "未知";
        }

        return provinceFix(str);
    }

    public static String[] getProvinceAndCityByIp(String ip) throws Exception {
        IPLocation ipLocation = getIpLocationByIp(ip);
        String str = null;
        if(ipLocation != null) {
            String area = ipLocation.getAreaStr();
            if(area != null && !area.isEmpty()){
                String province = provinceFix(area.substring(0, 2));
                int idx = area.indexOf("市");
                if(idx > 0){
                    String city = area.substring(0,idx+1);
                    return new String[]{province,city};
                }else return new String[]{province,area};
            }else return null;
        } else {
            return null;
        }
    }

    public static String getIspByIp(String ip) throws Exception {
        IPLocation ipLocation = getIpLocationByIp(ip);
        String str = null;
        if(ipLocation != null) {
            str = ipLocation.getIsp();
        } else {
            str = "未知";
        }
        if(str == null) str = "未知";
        return str;
    }

    public static String getWholeLocationByIp(String ip) throws Exception {
        IPLocation ipLocation = getIpLocationByIp(ip);
        String str = null;
        if(ipLocation != null) {
            str = ipLocation.getAreaStr();
        } else {
            str = "未知";
        }

        return provinceFix(str);
    }

    public static String provinceFix(String province){
        if(province == null){
            return "未知";
        }else if(province.equals("黑龙")){
            return  "黑龙江";
        }else if(province.equals("内蒙")){
            return "内蒙古";
        }else return province;
    }

    public static void main(String[] args) throws Exception {
        String[] str = new String[]{"115.204.95.126", "218.75.4.167", "116.226.226.173", "210.22.101.26", "110.179.240.61"};
        String[] var5 = str;
        int var4 = str.length;

        for(int var3 = 0; var3 < var4; ++var3) {
            String ip = var5[var3];
            System.out.println(getIspByIp(ip));
        }

    }


}

