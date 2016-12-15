package com.moretv.bi.report.medusa.dataAnalytics

import com.moretv.bi.util.DBOperationUtils

/**
 * Created by Administrator on 2016/9/12.
 * 该对象用于更新相关数据库的数据
 */
object updateDB {
  def main(args: Array[String]) {
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    val updateSQL1 = "update "
  }

}
