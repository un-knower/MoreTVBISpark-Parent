//package com.moretv.bi.tag
//
//import java.sql.DriverManager
//import java.text.SimpleDateFormat
//import java.util.Calendar
//import java.util.regex.Pattern
//
//import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
//import com.moretv.bi.util.{DateFormatUtils, DBOperationUtils, ParamsParseUtil, SparkSetting}
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.JdbcRDD
//import org.apache.spark.sql.SQLContext
//
///**
// * Created by Will on 2015/4/18.
// */
//object CommentTotalAccount extends BaseClass{
//
//  def main(args: Array[String]) {
//    config.setAppName("CommentTotalAccount")
//    ModuleClass.executor(this,args)
//  }
//  override def execute(args: Array[String]) {
//
//    ParamsParseUtil.parse(args) match {
//      case Some(p) => {
//        val yesterday = Calendar.getInstance()
//        val formatCN = new SimpleDateFormat("yyyy-MM-dd")
//        yesterday.add(Calendar.DAY_OF_MONTH, -p.whichDay)
//        val yesterdayCN = formatCN.format(yesterday.getTime)
//
//        val path = "/mbi/parquet/operation-acw/"+p.startDate+"/part-*"
//        val programMap = sqlContext.read.load(path).filter("event='comment'").select("accountId").map(e => e.getInt(0)+"").filter(_!=0).distinct()
//        val userIdRDD = new JdbcRDD(sc, ()=>{
//          Class.forName("com.mysql.jdbc.Driver")
//          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
//        },
//          "SELECT accountId FROM `commentTotalAccountId` WHERE ID >= ? AND ID <= ?",
//          1, Int.MaxValue.toLong, 10,
//          r=>r.getString(1)).distinct()
//        val resultRDD = programMap.subtract(userIdRDD)
//
//        //save date
//        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
//        //delete old data
//        if(p.deleteOld) {
//          val date = DateFormatUtils.toDateCN(p.startDate, -1)
//          val oldSql = s"delete from commentTotalAccounts where day = '$date'"
//          util.delete(oldSql)
//        }
//        val driver = "com.mysql.jdbc.Driver"
//        val url = "jdbc:mysql://10.10.2.15:3306/bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
//        val username = "bi"
//        val password = "mlw321@moretv"
//        Class.forName(driver)
//        val conn = DriverManager.getConnection(url, username, password)
//        val sql = "INSERT INTO bi.commentTotalAccountId(day,accountId) values(?,?)"
//        val preStm = conn.prepareStatement(sql)
//        val result = resultRDD.collect()
//        result.foreach(x => {
//          preStm.setString(1,yesterdayCN)
//          preStm.setString(2,x)
//          preStm.addBatch()
//        })
//        preStm.executeBatch()
//        //保存累计人数
//        val sql2 = "INSERT INTO bi.commentTotalAccounts(day,account_num) select '"+yesterdayCN+"', count(0) from bi.commentTotalAccountId where day <= '" + yesterdayCN +"'"
//        val preStm2 = conn.createStatement()
//        preStm2.execute(sql2)
//        preStm.close()
//        preStm2.close()
//        conn.close()
//      }
//      case None =>{
//        throw new RuntimeException("At least need param --excuteDate.")
//      }
//    }
//  }
//}
