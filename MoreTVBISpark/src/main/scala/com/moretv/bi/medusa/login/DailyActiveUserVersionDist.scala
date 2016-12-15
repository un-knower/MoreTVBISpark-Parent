package com.moretv.bi.medusa.login

import java.lang.{Long => JLong}
import com.moretv.bi.medusa.util.PMUtils
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/2/16.
  */
object DailyActiveUserVersionDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(DailyActiveUserVersionDist,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"
        val batch = 1

        val logRdd = sqlContext.read.load(inputPath).
          select("productModel","version","mac").
          filter("productModel is not null").
          map(row => (row.getString(0).toUpperCase,row.getString(1),row.getString(2))).
          filter(x => PMUtils.pmfilter(x._1)).cache()

        //active user and active num by productModel
        val loginNums = logRdd.map(x => ((x._1,x._2),x._3)).countByKey()
        val userNums = logRdd.distinct().map(x => ((x._1,x._2),x._3)).countByKey()

        val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld){
          val sqlDelete = "delete from version_dist_product_model_m3u where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = s"insert into version_dist_product_model_m3u(day,batch,product_model,version,user_num,login_num) values(?,$batch,?,?,?,?)"
        userNums.foreach(x => {
          val key = x._1
          val usernum = x._2
          val loginnum = loginNums(key)
          db.insert(sqlInsert,day,key._1,key._2,new JLong(usernum),new JLong(loginnum))
        })


        //union active user and active num
        val unionRdd = logRdd.map(x => (x._2,x._3)).cache()
        val unionLoginNums = unionRdd.countByKey()
        val unionUserNums = unionRdd.distinct().countByKey()

        if(p.deleteOld){
          val sqlDelete = "delete from version_dist_m3u where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsertTotal = s"insert into version_dist_m3u(day,batch,version,user_num,login_num) values(?,$batch,?,?,?)"
        unionUserNums.foreach(x => {
          val key = x._1
          val usernum = x._2
          val loginnum = unionLoginNums(key)
          db.insert(sqlInsertTotal,day,key,new JLong(usernum),new JLong(loginnum))
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
