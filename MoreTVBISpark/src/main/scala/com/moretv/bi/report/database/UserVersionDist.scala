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
object UserVersionDist extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(UserVersionDist,args)
  }
  override def execute(args: Array[String]) {

    val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
    val sqlIds = "SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account` "
    val ids = db.selectOne(sqlIds)
    val min = ids(0).toString.toLong
    val max = ids(1).toString.toLong
    val sqlRdd = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      "SELECT current_version,mac FROM `mtv_account` WHERE ID >= ? AND ID <= ?",
      min,
      max,
      1000,
      r=>(r.getString(1),r.getString(2)))

    val result = sqlRdd.map(x => {
      val version = x._1
      val apkVersion = getApkVersion(version)
      if(apkVersion != null) (apkVersion,x._2) else null
    }).filter(_ != null).distinct().countByKey()
    val out = new PrintWriter("/script/bi/moretv/liankai/file/UserVersionDist.csv")
    result.toList.sortBy(-_._2).foreach(x => {
      out.println(x._1+","+x._2)
    })
    out.close()
    println(result)

  }

  def getApkVersion(version:String) = {
    if(version != null ){
      if(version.startsWith("MoreTV_")){
        if(version.contains("Music") || version.contains("Comic") || version.contains("Kids")) null
        else {
          val idx = version.lastIndexOf("_")
          version.substring(idx+1)
        }
      }else null
    }else "无版本号"
  }

}
