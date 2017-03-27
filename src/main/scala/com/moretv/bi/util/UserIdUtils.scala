package com.moretv.bi.util

/**
 * Created by Will on 2015/7/17.
 */
object UserIdUtils {
  def getUserId(userIdStr:String)= {
    userIdStr.substring(0,32)
  }
  def getAccountId(userIdStr:String)={
    val temp = userIdStr.split("-")
    temp(1)
  }
  def getGroupId(userIdStr:String)={
    val temp = userIdStr.split("-")
    temp(2)
  }

  def userId2Long(userId:String) = {
    if(userId != null){
      var hash:Long = 0L
      for(i<- 0 until userId.length){
        hash = 47*hash + userId.charAt(i).toLong
      }

      hash = math.abs(hash)

      if(hash<1000000000){
        hash = hash + 1000000000*(hash+"").charAt(0).toInt
      }
      hash
    }else 0L
  }

}
