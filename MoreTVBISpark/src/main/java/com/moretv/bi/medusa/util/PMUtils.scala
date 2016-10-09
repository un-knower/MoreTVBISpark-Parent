package com.moretv.bi.medusa.util

/**
  * Created by Will on 2016/3/22.
  */
object PMUtils {

  val pmList = List("WE20S","M321","LETVNEWC1S","MAGICBOX_M13","MIBOX3")

  def pmfilter(pm:String) = pmList.contains(pm)
}
