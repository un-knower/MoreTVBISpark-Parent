package com.moretv.bi.user

import java.sql.DriverManager
import java.lang.{Long => JLong}

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/2/5.
 */
object YunOSNewUser extends BaseClass{

  val seriesArray = Array("Alibaba","YunOS")

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "2g").
      set("spark.cores.max", "30").
      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(YunOSNewUser,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {


        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val ids = util.selectOne(s"SELECT MIN(id),MAX(id) FROM tvservice.mtv_account WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59'")
        val sqlRDD = new JdbcRDD(sc, ()=>{
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
        },
          s"SELECT current_version,mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$day'",
          ids(0).toString.toLong,
          ids(1).toString.toLong,
          30,
          r=>(r.getString(1),r.getString(2))).map(t => if(matchLog(t._1)) t._2 else null).
          filter(_ != null).distinct()


        val userNum = sqlRDD.count()

        sc.stop()

        if(p.deleteOld) {
          val oldSql = s"delete from bi.yunos_new_user where day = '$day'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO bi.yunos_new_user(day,user_num) VALUES(?,?)"
        util.insert(sql,day,new JLong(userNum))

      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

  def matchLog(productModel:String) = {
    if(productModel != null){
      seriesArray.exists(s => productModel.contains(s))
    }else false
  }

}
