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
object DailyActiveUser extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(DailyActiveUser,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"
        val batch = 1

        val logRdd = sqlContext.read.load(inputPath).
          select("productModel","mac").
          filter("productModel is not null").
          map(row => (row.getString(0).toUpperCase,row.getString(1))).
          filter(x => PMUtils.pmfilter(x._1)).cache()

        //active user and active num by productModel
        val loginNums = logRdd.countByKey()
        val userNums = logRdd.distinct().countByKey()

        val db = new DBOperationUtils("medusa")
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld){
          val sqlDelete = "delete from user_trend_product_model_m3u where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = s"insert into user_trend_product_model_m3u(day,batch,product_model,new_num,user_num,login_num,total_user_num) values(?,$batch,?,?,?,?,?)"
        userNums.foreach(x => {
          val productModel = x._1
          val usernum = x._2
          val loginnum = loginNums(productModel)
          val totalusernum = db.selectOne("select count(distinct mac) from tvservice.mtv_account " +
            s"where openTime <= '$day 23:59:59' and product_model = '$productModel'")(0).toString.toLong
          val newusernum = db.selectOne("select count(distinct mac) from tvservice.mtv_account " +
            s"where openTime between '$day 00:00:00' and '$day 23:59:59' and product_model = '$productModel'")(0).toString.toLong
          db.insert(sqlInsert,day,productModel,new JLong(newusernum),new JLong(usernum),new JLong(loginnum),new JLong(totalusernum))
        })


        //union active user and active num
        val unionRdd = logRdd.map(_._2).cache()
        val unionLoginNum = unionRdd.count()
        val unionUserNum = unionRdd.distinct().count()

        if(p.deleteOld){
          val sqlDelete = "delete from user_trend_m3u where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsertTotal = s"insert into user_trend_m3u(day,batch,new_num,user_num,login_num,total_user_num) values(?,$batch,?,?,?,?)"
        val totalusernum = db.selectOne("select count(distinct mac) from tvservice.mtv_account " +
          s"where openTime <= '$day 23:59:59' and product_model in ('we20s','M321','LetvNewC1S','MagicBox_M13','MiBOX3')")(0).toString.toLong
        val newusernum = db.selectOne("select count(distinct mac) from tvservice.mtv_account " +
          s"where openTime between '$day 00:00:00' and '$day 23:59:59' and product_model in ('we20s','M321','LetvNewC1S','MagicBox_M13','MiBOX3')")(0).toString.toLong
        db.insert(sqlInsertTotal,day,new JLong(newusernum),new JLong(unionUserNum),new JLong(unionLoginNum),new JLong(totalusernum))


        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }


}
