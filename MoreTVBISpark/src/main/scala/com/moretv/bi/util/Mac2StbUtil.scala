package com.moretv.bi.util

/**
  * Created by Will on 2016/2/16.
  * 将mac地址映射为特定的盒子品牌
  */
object Mac2StbUtil {

  private val macMap = Map("DCC0DB" -> "开博尔",
                            "E497F0" -> "VLC",
                            "005056" -> "VLC",
                            "005057" -> "VLC",
                            "ACDBDA" -> "金亚太/美如画",
                            "000DFE" -> "金亚太/美如画",
                            "C80E77" -> "乐视致新",
                            "1048B1" -> "小米/多看",
                            "983B16" -> "小米/多看",
                            "6CFAA7" -> "小米/多看",
                            "0066BB" -> "海美迪",
                            "0066CC" -> "海美迪",
                            "0066CD" -> "海美迪",
                            "0066CE" -> "海美迪",
                            "0066BA" -> "海美迪",
                            "001518" -> "天敏",
                            "00CE39" -> "迈乐",
                            "00116D" -> "杰科",
                            "F0620D" -> "亿格瑞",
                            "00D1D2" -> "迪优美特",
                            "0070B1" -> "迪优美特",
                            "0070B2" -> "迪优美特",
                            "14CF92" -> "TPLink",
                            "00235A" -> "我播",
                            "0022FF" -> "威堡",
                            "0CC655" -> "易视腾",
                            "0016F5" -> "华录",
                            "3828EA" -> "网讯",
                            "BC83A7" -> "创维",
                            "001A9A" -> "创维",
                            "1CA770" -> "创维",
                            "5CC6D0" -> "创维数字",
                            "001A34" -> "康佳",
                            "001449" -> "长虹",
                            "6488FF" -> "长虹",
                            "90CF7D" -> "海信",
                            "C816BD" -> "海信",
                            "E0BC43" -> "海尔",
                            "408BF6" -> "TCL",
                            "0026EA" -> "同方(鑫岳)",
                            "F20207" -> "台湾惟其",
                            "100102" -> "忆典",
                            "0014E9" -> "某白牌2",
                            "200000" -> "minix",
                            "00-11-7F" -> "英菲克",
                            "003882" -> "英菲克",
                            "00245A" -> "熊猫",
                            "6C5AB5" -> "TCL盒子",
                            "0CF0B4" -> "迈科",
                            "543530" -> "鸿海科技",
                            "BC261D" -> "TECON香港",
                            "9CF8DB" -> "eyunmei")

  def mac2Stb(mac:String) = {
    if(mac == null) "不合法"
    else if(mac.length < 8 || mac.length >= 32) "未知"
    else {
      val mac6 = mac.substring(0,6).toUpperCase
      macMap.get(mac6) match {
        case Some(m) => m
        case None => {
          val mac8 = mac.substring(0,8).toUpperCase
          macMap.get(mac8) match {
            case Some(n) => n
            case None => "未知"
          }
        }
      }
    }
  }
}
