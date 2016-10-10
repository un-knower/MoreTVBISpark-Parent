package com.moretv.bi.user

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._
/**
  * 创建人黄虎
  * 创建时间 2016:07:06
  * 程序作用：统计用户新增活跃终端分布
  */
object ProductBrandDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ProductBrandDist,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {
        val inputDateActive = p.startDate
        val inputPathActive = s"/log/moretvloginlog/parquet/$inputDateActive/loginlog"

        val day = DateFormatUtils.toDateCN(inputDateActive,-1)
        val inputDateNew = day.replace("-","")

        val inputPathNew= s"/log/dbsnapshot/parquet/$inputDateNew/moretv_mtv_account"
        sqlContext.udf.register("getBrand",ProductModelUtils.getProductBrand _)

        sqlContext.read.load(inputPathActive).select("productModel","mac").registerTempTable("log_data")
        sqlContext.read.load(inputPathNew).select("openTime","product_model","mac").registerTempTable("log_data1")

        val resultActiveMap = sqlContext.sql("select getBrand(productModel),count(distinct mac) from log_data group by getBrand(productModel)").
          collectAsList().map(row => (row.getString(0),row.getLong(1))).toMap
        val resultNewMap = sqlContext.sql("select getBrand(product_model) ,count(distinct mac) from log_data1 " +
          s" where openTime >= '$day 00:00:00' and openTime <= '$day 23:59:59' group by getBrand(product_model)").
          collectAsList().map(row => (row.getString(0),row.getLong(1))).toMap

        val db = DataIO.getMySqlOps("moretv_medusa_mysql")
        if(p.deleteOld){
          val sqlDelete = "delete from product_brand_dau_new where day = ?"
          db.delete(sqlDelete,day)
        }

        val keys = resultActiveMap.keySet.union(resultNewMap.keySet)
        val sqlInsert = "insert into product_brand_dau_new(day,product_brand,active_num,new_num) values(?,?,?,?)"
        keys.foreach(key => {
          val activeNum = resultActiveMap.getOrElse(key,0)
          val newNum = resultNewMap.getOrElse(key,0)
          db.insert(sqlInsert,day,key,activeNum,newNum)
        })

        db.destory()

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
