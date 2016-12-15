package com.moretv.bi.report

import java.io.PrintWriter
import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, ProductModelUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/5/8.
 * 用于统计当前数据库中用户的终端型号和终端品牌分布
 */
object UserVersionDist extends SparkSetting{

  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    //设置Task执行时依赖的jar文件
    sc.addJar("hdfs://hans/lib/common/commons-dbutils-1.6.jar")
    val numOfPartition = 300

    val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
    val maxId = getMaxId(util)
    util.destory()
    val jdbcRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      "SELECT current_version,mac FROM `mtv_account` WHERE id >= ? AND id <= ?",
      5,
      maxId,
      numOfPartition,
      r=>(r.getString(1),r.getString(2))).map(x => {
          val version = x._1
          if(version != null){
            val idx = version.lastIndexOf("_")
            (version.substring(idx+1),x._2)
          }else ("NULL",x._2)
        }).cache()
    val modelMap = jdbcRDD.distinct().countByKey()
    val file = "/script/bi/moretv/liankai/file/VersionDistTotal.csv"
    val out = new PrintWriter(file,"GBK")
    modelMap.foreach(
      e =>
        out.println(e._1+","+e._2)
    )
    out.close()

  }

  def getMaxId(util: DBOperationUtils) = {
    val sql = "SELECT MAX(id) FROM mtv_account"
    val arr = util.selectOne(sql)
    arr(0).toString.toLong
  }
}
