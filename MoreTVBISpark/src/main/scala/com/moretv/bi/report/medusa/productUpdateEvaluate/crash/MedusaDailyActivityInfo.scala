package com.moretv.bi.report.medusa.productUpdateEvaluate.crash

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MedusaDailyActivityInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate

        val medusaDir = "/log/medusa/parquet"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val medusaDailyActiveInput = s"$medusaDir/$date/*/"
          val medusaDailyEnterInput = s"$medusaDir/$date/enter/"

          sqlContext.read.parquet(medusaDailyActiveInput).select("userId","apkVersion","buildDate","productModel").
            filter("length(buildDate)<20").registerTempTable("all_log")
          sqlContext.read.parquet(medusaDailyEnterInput).select("userId","apkVersion","buildDate","productModel").
            filter("length(buildDate)<20").registerTempTable("enter_log")


          if(p.deleteOld){
            val deleteSql = "delete from medusa_product_update_daily_active_user_login_num_each_day where day=?"
            util.delete(deleteSql,insertDate)
          }

          val sqlInsert = "insert into medusa_product_update_daily_active_user_login_num_each_day(day,apk_version," +
            "buildDate,productModel,active_user,active_num) values (?,?,?,?,?,?)"

          // 计算每个apkVersion、不同buildDate不同productModel的日活登录次数
          val userRdd1=sqlContext.sql("select apkVersion,buildDate,productModel,count(distinct userId) from all_log " +
            " where length(apkVersion)=5 group by apkVersion,buildDate,productModel").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val numRdd1 = sqlContext.sql("select apkVersion,buildDate,productModel,count(userId) from enter_log where length" +
            "(apkVersion)=5 group by apkVersion,buildDate,productModel").map(e=>(e.getString(0),e.getString(1),e.getString
            (2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val rdd1 = userRdd1 join numRdd1
          rdd1.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1._1,e._1._2,e._1._3,new JLong(e._2._1), new JLong(e._2._2))
          })

          // 计算每个apkVersion、不同buildDate所有productModel的日活登录次数
          val userRdd2=sqlContext.sql("select apkVersion,buildDate,'All' as productModel,count(distinct userId) from " +
            "all_log where length(apkVersion)=5 group by apkVersion,buildDate").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val numRdd2 = sqlContext.sql("select apkVersion,buildDate,'All' as productModel,count(userId) from enter_log " +
            "where length (apkVersion)=5 group by apkVersion,buildDate").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val rdd2 = userRdd2 join numRdd2
          rdd2.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1._1,e._1._2,e._1._3,new JLong(e._2._1), new JLong(e._2._2))
          })

          // 计算每个productModel所有apkVersion的日活登录次数
          val userRdd3=sqlContext.sql("select 'All' as apkVersion,'All' as buildDate,productModel,count(distinct " +
            "userId) from all_log where length(apkVersion)=5 group by productModel").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val numRdd3 = sqlContext.sql("select 'All' as apkVersion,'All' as buildDate, productModel,count(userId) from " +
            "enter_log where length (apkVersion)=5 group by productModel").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val rdd3 = userRdd3 join numRdd3
          rdd3.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1._1,e._1._2,e._1._3,new JLong(e._2._1), new JLong(e._2._2))
          })


          // 计算所有版本的日活人数与登录次数
          val allUserRdd=sqlContext.sql("select 'All' as apkVersion,'All' as buildDate,'All' as productModel," +
            "count(distinct userId) from all_log where length(apkVersion)=5").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val allNumRdd = sqlContext.sql("select 'All' as apkVersion,'All' as buildDate,'All' as productModel,count" +
            "(userId) from enter_log where length (apkVersion)=5").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val allRdd = allUserRdd join allNumRdd
          allRdd.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1._1,e._1._2,e._1._3,new JLong(e._2._1), new JLong(e._2._2))
          })

          // 计算除了某个版本之外的日活人数与登录次数
          if(p.apkVersion!=""){
            val apkVersion = p.apkVersion

            // 计算非apkVersion、buildDate不同productModel的日活登录次数
            val userRdd1=sqlContext.sql(s"select '' as apkVersion,'' as buildDate,productModel,count(distinct userId) from" +
              s" all_log where length(apkVersion)=5 and apkVersion!='$apkVersion' group by productModel").
              map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
            val numRdd1 = sqlContext.sql("select '' as apkVersion,'' as buildDate,productModel,count(userId) from " +
              "enter_log " +
              "where length" +
              "(apkVersion)=5 group by productModel").map(e=>(e.getString(0),e.getString(1),e.getString
              (2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
            val rdd1 = userRdd1 join numRdd1
            rdd1.collect().foreach(e=>{
              util.insert(sqlInsert,insertDate,"Non-".concat(apkVersion),"All",e._1._3,new JLong(e._2._1), new JLong(e._2
                ._2))
            })

            // 计算非apkVersion所有productModel的日活登录次数
            val userRdd2=sqlContext.sql(s"select '' as apkVersion,'' as buildDate,'All' as productModel,count(distinct " +
              s"userId) from all_log where length(apkVersion)=5 and apkVersion!='$apkVersion'").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
            val numRdd2 = sqlContext.sql(s"select '' as apkVersion,'' as buildDate,'All' as productModel,count(userId) " +
              s"from enter_log where length (apkVersion)=5 and apkVersion!='$apkVersion'").map(e=>(e
              .getString(0),e.getString(1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
            val rdd2 = userRdd2 join numRdd2
            rdd2.collect().foreach(e=>{
              util.insert(sqlInsert,insertDate,"Non-".concat(apkVersion),"All",e._1._3,new JLong(e._2._1), new JLong
              (e._2._2))
            })
          }

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
