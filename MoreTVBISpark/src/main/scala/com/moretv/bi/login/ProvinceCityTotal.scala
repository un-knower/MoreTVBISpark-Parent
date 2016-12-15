package com.moretv.bi.login

import java.lang.{Long => JLong}
import java.sql.DriverManager

import com.moretv.bi.constant.Database
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/2/16.
  */
object ProvinceCityTotal extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(ProvinceCityTotal,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val whichMonth = p.whichMonth
        val s = sqlContext
        import s.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val sqlSelect = "select min(id),max(id) from mtv_account where left(openTime,7) <= ?"
        val ids = util.selectOne(sqlSelect,whichMonth)

        val df = new JdbcRDD(sc, ()=>{
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
        },
          s"SELECT ip,user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,7) <= '$whichMonth'",
          ids(0).toString.toLong,
          ids(1).toString.toLong,
          300,
          r=>(r.getString(1),r.getString(2))).distinct().
          map(t => {
            val arr = IPUtils.getProvinceAndCityByIp(t._1)
            if(arr != null) {
              val Array(province,city) = arr
              (province,city,t._2)
            }else null
          }).
          filter(_ != null).toDF("province","city","userId").cache()

        df.registerTempTable("log_data")
        val result = sqlContext.sql("select province,city,count(userId) from log_data group by province,city").collectAsList()

        val db = new DBOperationUtils(Database.BI)
        val sqlInsert = "insert into province_city_total(month,province,city,user_num) values(?,?,?,?)"
        result.foreach(row => {
          val province = row.getString(0)
          val city = row.getString(1)
          val userNum = row.getLong(2)
          db.insert(sqlInsert,whichMonth,province,city,new JLong(userNum))

        })
        if(p.deleteOld){
          val sqlDelete = "delete from province_city_total where month = ?"
          db.delete(sqlDelete,whichMonth)
        }

        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
