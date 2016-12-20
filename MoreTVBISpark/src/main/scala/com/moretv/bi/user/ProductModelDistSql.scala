package com.moretv.bi.user

import java.sql.DriverManager
import java.util.Date

import com.moretv.bi.util.FileUtils._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, ProductModelUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by Will on 2015/8/5.
  */
object ProductModelDistSql extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ProductModelDistSql,args)
  }
   override def execute(args: Array[String]) {
     val maxId = getMaxId
     val jdbcRdd = new JdbcRDD(sc,()=>{
       Class.forName("com.mysql.jdbc.Driver")
       DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
         "bi", "mlw321@moretv")
     },
       "SELECT product_model FROM `mtv_account` WHERE ID >= ? AND ID <= ? " +
         "AND openTime BETWEEN '2015-08-05 00:00:00' AND '2015-08-05 23:59:59' " +
         "AND current_version = ‘’",
       1,
       maxId,
       100,
       r=>r.getString(1)).cache()

     val modelResult = jdbcRdd.countByValue()
     val brandResult = jdbcRdd.map(ProductModelUtil.getProductBrand).countByValue()

     withCsvWriterOld("/home/moretv/liankai.tmp/share_dir/productModelDist.csv"){
       out => {
         out.println(new Date())
         modelResult.foreach(x => {
           out.println(x._1+","+x._2)
         })
       }
     }

     withCsvWriterOld("/home/moretv/liankai.tmp/share_dir/productBrandDist.csv"){
       out => {
         out.println(new Date())
         brandResult.foreach(x => {
           out.println(x._1+","+x._2)
         })
       }
     }

   }

   def getMaxId = {
     val sql ="SELECT MAX(id) FROM tvservice.`mtv_account`"
     val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
     util.selectOne(sql)(0).toString.toLong
   }

 }
