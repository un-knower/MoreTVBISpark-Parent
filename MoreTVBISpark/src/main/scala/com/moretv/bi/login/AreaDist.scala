package com.moretv.bi.login

import java.sql.DriverManager

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.rdd.JdbcRDD

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/06/10.
  * 统计各地区的新增用户、活跃用户、活跃次数、累计用户数
  *
  */
object AreaDist extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(AreaDist,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._
        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"
        val day = DateFormatUtils.toDateCN(inputDate,-1)


        val logRdd = sqlContext.read.load(inputPath).select("ip","userId").
          map(e => (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(e.getString(0))),e.getString(1))).
          toDF("ip","userId").registerTempTable("active_data")
        val activeRows = sqlContext.sql("select ip,count(distinct userId),count(userId) from active_data group by ip").collectAsList()

        val util = DataIO.getMySqlOps("moretv_tvservice_mysql")
        val sqlSelect = "select min(id),max(id) from mtv_account where left(openTime,10) = ?"
        val ids = util.selectOne(sqlSelect,day)
        util.destory()

        val totalMap = new JdbcRDD(sc, ()=>{
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
        },
          s"SELECT ip,user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) <= '$day' and ip is not null",
          1,
          ids(1).toString.toLong,
          300,
          r=>(r.getString(1),r.getString(2))).distinct().
          map(t => {
            (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(t._1)),t._2)
          }).countByKey()

        val newMap = new JdbcRDD(sc, ()=>{
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
        },
          s"SELECT ip,user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$day' and ip is not null",
          ids(0).toString.toLong,
          ids(1).toString.toLong,
          300,
          r=>(r.getString(1),r.getString(2))).distinct().
          map(t => {
            (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(t._1)),t._2)
          }).countByKey()

        val db = DataIO.getMySqlOps("moretv_eagletv_mysql")
        if(p.deleteOld){
          val sqlDelete = "delete from login_detail where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into login_detail(day,area,new_num,user_num,log_num,active_num,total_num) values(?,?,?,?,?,?,?)"
        val activeMap = activeRows.map(row => (row.getString(0),(row.getLong(1),row.getLong(2)))).toMap
        totalMap.foreach(x => {
          val province = x._1
          val totalNum = x._2
          val (userNum,logNum) = activeMap.getOrElse(province,(0l,0l))
          val newNum = newMap.getOrElse(province,0l)
          val activeNum = userNum - newNum
          db.insert(sqlInsert,day,province,newNum,userNum,logNum,activeNum,totalNum)
        })
        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}

