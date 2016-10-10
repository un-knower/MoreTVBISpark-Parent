package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import com.moretv.bi.util.IPLocationUtils.IPOperatorsUtil
import com.moretv.bi.util.LiveCodeToNameUtils

/**
 * Created by Administrator on 2016/7/27.
 */
object LiveNameTest {

  def main(args: Array[String]) {
    (0 until args.length).foreach(i=>{
      val sid = args(i)
      val name = LiveCodeToNameUtils.getChannelNameBySid(sid)
      println(name)
    })

  }

}
