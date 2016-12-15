package com.moretv.bi.login

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/2/16.
  */
object GenerateMtvLoginLogData extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(GenerateMtvLoginLogData,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"

        val logRdd = sqlContext.read.load(inputPath).select("mac").
          map(mac => (Mac2StbUtil.mac2Stb(mac.getString(0)),mac.getString(0))).cache()
        val loginNums = logRdd.countByKey()
        val userNums = logRdd.distinct().countByKey()

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld){
          val sqlDelete = "delete from mtv_loginLog where date = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into mtv_loginLog(date,stbType,usernum,loginnum) values(?,?,?,?)"
        userNums.foreach(x => {
          val stbType = x._1
          val usernum = x._2
          val loginnum = loginNums(stbType)
          db.insert(sqlInsert,day,stbType,new Integer(usernum.toInt),new Integer(loginnum.toInt))
        })
        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
