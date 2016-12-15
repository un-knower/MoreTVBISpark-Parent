package com.moretv.bi.temp

import java.io.PrintWriter
import java.sql.DriverManager

import com.moretv.bi.util.{DBOperationUtils, SparkSetting}
import com.moretv.ip.IPUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/5/8.
 * 用于统计当前数据库中用户的地区分布
 */
object UserAlibabaVersionDistribution extends SparkSetting{

  /**
   * 主体思路：
   * 1.查询出计算时数据库中的最大id
   * 2.创建jdbcRDD，只获取ip信息即可，因为在用户表中一行记录就代表一个用户
   * 3.通过工具类将ip映射为省份地区
   * 4.对上述数据进行groupBy，计算出每个地区的用户人数
   * @param args
   */
  def main(args: Array[String]) {
    config.setAppName("UserAreaDistribution")
    val sc = new SparkContext(config)
    //设置Task执行时依赖的jar文件
    sc.addJar("/home/moretv/mbi/lib/common/commons-dbutils-1.6.jar")
    val numOfPartition = 40

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
      r=>(r.getString(1),r.getString(2)))
    val result = jdbcRDD.filter(t => t != null && t._1.contains("Alibaba")).distinct().groupByKey().
      map(t => (t._1,t._2.size)).collect()

    val file = s"/home/moretv/liankai.tmp/share_dir/UserAlibabaVersionDistribution.csv"
    val out = new PrintWriter(file,"GBK")
    result.foreach(
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
