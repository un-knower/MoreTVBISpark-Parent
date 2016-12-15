package com.moretv.bi.temp.medusa

import java.lang.{Long => JLong}
import com.moretv.bi.medusa.util.PMUtils
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/3/23.
  */
object VVDurationM3UMoretv extends SparkSetting{

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val inputDate = p.startDate
        val batch = 1
        val inputPath = s"/mbi/parquet/playview/$inputDate"
        val logRdd = sqlContext.read.load(inputPath).
          select("productModel", "duration", "userId").
          filter("productModel is not null and duration between 0 and 10800").
          map(row => (row.getString(0).toUpperCase, row.getInt(1), row.getString(2))).
          filter(x => PMUtils.pmfilter(x._1)).cache()
        logRdd.toDF("productModel", "duration", "userId").registerTempTable("log_data")
        val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val day = DateFormatUtils.toDateCN(inputDate, -1)

        //avg usage duration by productModel
        val result = sqlContext.sql("select productModel,count(distinct userId),sum(duration) from log_data group by productModel").collect()
        if (p.deleteOld) {
          val sqlDelete = "delete from vv_duration_product_model_m3u where day = ?"
          db.delete(sqlDelete, day)
        }
        val sqlInsert = "insert into vv_duration_product_model_m3u(day,batch,type,product_model,user_num,duration) " +
          s"values(?,$batch,'moretv',?,?,?)"
        val sqlInsertTotal = "insert into vv_duration_product_model_m3u(day,batch,type,product_model,user_num,duration) " +
          s"values(?,$batch,'total',?,?,?)"
        result.foreach(row => {
          db.insert(sqlInsert,day,row.getString(0),new JLong(row.getLong(1)),new JLong(row.getLong(2)))
          db.insert(sqlInsertTotal,day,row.getString(0),new JLong(row.getLong(1)),new JLong(row.getLong(2)))
        })
        //union avg usage duration by productModel
        val resultUnion = sqlContext.sql("select count(distinct userId),sum(duration) from log_data").collect()
        if (p.deleteOld) {
          val sqlDelete = "delete from vv_duration_m3u where day = ?"
          db.delete(sqlDelete, day)
        }
        val sqlInsertUnion = s"insert into vv_duration_m3u(day,batch,type,user_num,duration) values(?,$batch,'moretv',?,?)"
        val sqlInsertUnionTotal = s"insert into vv_duration_m3u(day,batch,type,user_num,duration) values(?,$batch,'total',?,?)"
        resultUnion.foreach(row => {
          db.insert(sqlInsertUnion,day,new JLong(row.getLong(0)),new JLong(row.getLong(1)))
          db.insert(sqlInsertUnionTotal,day,new JLong(row.getLong(0)),new JLong(row.getLong(1)))
        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
