package com.moretv.bi.temp

import java.io.{PrintWriter, File}
import java.sql.{DriverManager, Connection}

import com.moretv.ip.IPUtils
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by Will on 2015/2/5.
 */
object UserAeraDistribution_old {


  def main(args: Array[String]) {
    val conf = new SparkConf().
      setMaster("spark://10.10.2.14:7077").
      setAppName("UserAeraDistribution-用户地区分布").
      set("spark.executor.memory", "1g").
      set("spark.cores.max", "20").
      set("spark.scheduler.mode","FAIR")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    val user = "bi"
    val password = "mlw321@moretv"
    Class.forName(driver)
    val conn = DriverManager.getConnection(url,user,password)

    val ids = getMinAndMaxID(conn)
    val minId = ids(0)
    val maxId = ids(1)

    val sqlRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      "SELECT ip,mac FROM mtv_account WHERE id >= ? and id <= ? and mac IS NOT NULL",
      minId,
      maxId,
      60,
      r=>(r.getString(1),r.getString(2))).distinct()

    val result = sqlRDD.map(x => (ip2Aera(x._1),1)).countByKey()

    val file = new File("/home/moretv/liankai.tmp/share_dir/UserAeraDistribution.txt")
    val out = new PrintWriter(file)
    result.foreach(x => {
      out.println(x._1 + "\t" + x._2)
    })
    out.close()


  }

  def getMinAndMaxID(conn: Connection) = {
    val result = new Array[Long](2)
    val sql ="SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account`"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    rs.next()
    result(0) = rs.getLong(1)
    result(1) = rs.getLong(2)
    rs.close()
    stmt.close()
    result
  }

  def ip2Aera(ip:String):String = {
    IPUtils.getProvinceByIp(ip)
  }

}
