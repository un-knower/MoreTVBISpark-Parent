package com.moretv.bi.whiteMedusaVersionEstimate

/**
  * Created by Chubby on 2017/4/26.
  * 该类用于处理版本号信息，eg：MoreTV_TVApp3.0_Medusa_3.1.4->3.1.4
  */
object ApkVersionUtil {


  /**
    * 从apkSerial中提取出apkVersion
    * @param apkSerials
    * @return
    */
  def getApkVersion(apkSerials:String) = {
    if(apkSerials != null){
      if(apkSerials == "")
        "kong"
      else if(apkSerials.contains("_")){
        apkSerials.substring(apkSerials.lastIndexOf("_")+1)
      }else{
        apkSerials
      }
    }else
      "null"
  }

}
