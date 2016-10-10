package com.moretv.bi.login

import com.moretv.bi.constant.Database

import scala.collection.JavaConversions._
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.{Long => JLong}

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by Will on 2016/2/16.
  */
object ProvinceCityActiveMonth extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(ProvinceCityActiveMonth,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val whichMonth = p.whichMonth
        val inputPath = s"/log/moretvloginlog/parquet/$whichMonth*/loginlog"
        val s = sqlContext
        import s.implicits._

        val df = sqlContext.read.load(inputPath).select("ip","userId").
          map(e => {
            val arr = IPUtils.getProvinceAndCityByIp(e.getString(0))
            if(arr != null) {
              val Array(province,city) = arr
              (province,city,e.getString(1))
            }else null

          }).filter(_!=null).toDF("province","city","userId").cache()
        df.registerTempTable("log_data")
        val result = sqlContext.sql("select province,city,count(distinct userId),count(userId) from log_data group by province,city").collectAsList()

        val db = new DBOperationUtils(Database.BI)
        val sqlInsert = "insert into province_city_dist_month(month,province,city,user_num,login_num) values(?,?,?,?,?)"
        result.foreach(row => {
          val province = row.getString(0)
          val city = row.getString(1)
          val userNum = row.getLong(2)
          val loginNum = row.getLong(3)
          db.insert(sqlInsert,whichMonth,province,city,new JLong(userNum),new JLong(loginNum))

        })
        if(p.deleteOld){
          val sqlDelete = "delete from province_city_dist_month where month = ?"
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
