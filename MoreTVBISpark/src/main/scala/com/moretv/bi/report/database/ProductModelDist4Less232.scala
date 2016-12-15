package com.moretv.bi.report.database

import java.io.PrintWriter
import java.sql.DriverManager

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/3/24.
  */
object ProductModelDist4Less232 extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ProductModelDist4Less232,args)
  }

  override def execute(args: Array[String]) {

    val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
    val sqlIds = "SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account` " +
      "WHERE openTime between '2015-01-01 00:00:00' and '2016-03-24 00:00:00'"
    val ids = db.selectOne(sqlIds)
    val min = ids(0).toString.toLong
    val max = ids(1).toString.toLong
    val sqlRdd = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      "SELECT current_version,product_model,mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? and openTime between '2015-01-01 00:00:00' and '2016-03-24 00:00:00'",
      min,
      max,
      1000,
      r=>(r.getString(1),r.getString(2),r.getString(3)))

    val result = sqlRdd.filter(x => versionFilter(x._1)).map(x => (x._2,x._3)).distinct().countByKey()
    val out = new PrintWriter("/script/bi/moretv/liankai/file/ProductModelDist4Less232.csv")
    result.toList.sortBy(-_._2).foreach(x => {
      out.println(x._1+","+x._2)
    })
    out.close()

  }

  def versionFilter(version:String) = {
    if(version != null && version.startsWith("MoreTV_")){
      if(version.contains("Music") || version.contains("Comic") || version.contains("Kids")) false
      else {
        val idx = version.lastIndexOf("_")
        val apkVersion = version.substring(idx+1)
        if(apkVersion <= "2.3.2") true else false
      }
    }else false
  }

}
