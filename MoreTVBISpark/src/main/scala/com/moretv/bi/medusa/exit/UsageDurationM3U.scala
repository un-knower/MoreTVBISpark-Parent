package com.moretv.bi.medusa.exit

import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.PMUtils
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/3/23.
  */
object UsageDurationM3U extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(UsageDurationM3U,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._
        val inputDate = p.startDate
        val batch = 1
        val inputPath = s"/mbi/parquet/exit/$inputDate"
        val logRdd = sqlContext.read.load(inputPath).
          select("productModel", "duration", "userId").
          filter("productModel is not null and duration between 0 and 54000").
          map(row => (row.getString(0).toUpperCase, row.getInt(1), row.getString(2))).
          filter(x => PMUtils.pmfilter(x._1)).cache()
        logRdd.toDF.registerTempTable("log_data")
        val db = new DBOperationUtils("medusa")
        val day = DateFormatUtils.toDateCN(inputDate, -1)

        //avg usage duration by productModel
        val result = sqlContext.sql("select _1,count(distinct _3),sum(_2) from log_data group by _1").collect()
        if (p.deleteOld) {
          val sqlDelete = "delete from usage_duration_product_model_m3u where day = ?"
          db.delete(sqlDelete, day)
        }
        val sqlInsert = "insert into usage_duration_product_model_m3u(day,batch,type,product_model,user_num,duration) " +
          s"values(?,$batch,'moretv',?,?,?)"
        result.foreach(row => {
          db.insert(sqlInsert,day,row.getString(0),new JLong(row.getLong(1)),new JLong(row.getLong(2)))
        })
        //union avg usage duration by productModel
        val resultUnion = sqlContext.sql("select count(distinct _3),sum(_2) from log_data").collect()
        if (p.deleteOld) {
          val sqlDelete = "delete from usage_duration_m3u where day = ?"
          db.delete(sqlDelete, day)
        }
        val sqlInsertUnion = s"insert into usage_duration_m3u(day,batch,type,user_num,duration) values(?,$batch,'moretv',?,?)"
        resultUnion.foreach(row => {
          db.insert(sqlInsertUnion,day,new JLong(row.getLong(0)),new JLong(row.getLong(1)))
        })


        //medusa
        val inputPathMedusa = s"/log/medusa/parquet/$inputDate/exit"
        val logRddMedusa = sqlContext.read.load(inputPathMedusa).
          select("productModel", "duration", "userId").
          filter("productModel is not null and duration between 0 and 54000").
          map(row => (row.getString(0).toUpperCase, row.getLong(1), row.getString(2))).
          filter(x => PMUtils.pmfilter(x._1)).cache()
        logRddMedusa.toDF.registerTempTable("log_data_m")

        //avg usage duration by productModel
        val resultMedusa = sqlContext.sql("select _1,count(distinct _3),sum(_2) from log_data_m group by _1").collect()
        val sqlInsertMedusa = "insert into usage_duration_product_model_m3u(day,batch,type,product_model,user_num,duration) " +
          s"values(?,$batch,'medusa',?,?,?)"
        resultMedusa.foreach(row => {
          db.insert(sqlInsertMedusa,day,row.getString(0),new JLong(row.getLong(1)),new JLong(row.getLong(2)))
        })
        //union avg usage duration by productModel
        val resultUnionMedusa = sqlContext.sql("select count(distinct _3),sum(_2) from log_data_m").collect()
        val sqlInsertUnionMedusa = s"insert into usage_duration_m3u(day,batch,type,user_num,duration) values(?,$batch,'medusa',?,?)"
        resultUnionMedusa.foreach(row => {
          db.insert(sqlInsertUnionMedusa,day,new JLong(row.getLong(0)),new JLong(row.getLong(1)))
        })

        //total

        logRddMedusa.union(logRdd.map(x => (x._1,x._2.toLong,x._3))).toDF.registerTempTable("log_data_t")

        //avg usage duration by productModel
        val resultTotal = sqlContext.sql("select _1,count(distinct _3),sum(_2) from log_data_t group by _1").collect()
        val sqlInsertTotal = "insert into usage_duration_product_model_m3u(day,batch,type,product_model,user_num,duration) " +
          s"values(?,$batch,'total',?,?,?)"
        resultTotal.foreach(row => {
          db.insert(sqlInsertTotal,day,row.getString(0),new JLong(row.getLong(1)),new JLong(row.getLong(2)))
        })
        //union avg usage duration by productModel
        val resultUnionTotal = sqlContext.sql("select count(distinct _3),sum(_2) from log_data_t").collect()
        val sqlInsertUnionTotal = s"insert into usage_duration_m3u(day,batch,type,user_num,duration) values(?,$batch,'total',?,?)"
        resultUnionTotal.foreach(row => {
          db.insert(sqlInsertUnionTotal,day,new JLong(row.getLong(0)),new JLong(row.getLong(1)))
        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
