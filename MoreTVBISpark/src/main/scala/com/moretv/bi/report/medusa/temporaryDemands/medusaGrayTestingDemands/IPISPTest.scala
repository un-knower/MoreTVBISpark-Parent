package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import com.moretv.bi.util.IPLocationUtils.IPOperatorsUtil

/**
 * Created by Administrator on 2016/7/27.
 */
object IPISPTest {

  def main(args: Array[String]) {
    (0 until args.length).foreach(i=>{
      val ip = args(i)
      val begin = System.nanoTime()
      val isp = IPOperatorsUtil.getISPInfo(ip)
      val end = System.nanoTime()
      println(isp)
      println("Query time: ",end-begin)
    })

  }

}
