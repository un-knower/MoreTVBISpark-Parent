package com.moretv.bi.user

import java.sql.DriverManager

import com.moretv.bi.util.SparkSetting
import com.moretv.bi.util.IPUtils
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/9/12.
 */
object UserIspDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(UserIspDist,args)
  }
  override def execute(args: Array[String]) {
    val ids = getIDs
    val min = ids._1
    val max = ids._2
    val sqlRdd = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      "SELECT ip FROM `mtv_account` WHERE ID >= ? AND ID <= ?",
      min,
      max,
      1000,
      r=>r.getString(1))
    val result = sqlRdd.map(IPUtils.getIspByIp).countByValue()

    result.foreach(e => {
      println(e._1+","+e._2)
    })
  }

  def getIDs = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "bi", "mlw321@moretv")
    val sql ="SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account`"
    val stmt = conn.createStatement()
    val ids = stmt.executeQuery(sql)
    ids.next()
    (ids.getLong(1),ids.getLong(2))
  }
}
