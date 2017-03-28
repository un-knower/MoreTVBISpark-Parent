package com.moretv.bi.user

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._
/**
  * 创建人黄虎
  * 创建时间 2016:07:06
  * 程序作用：统计用户新增活跃终端分布
  */
object ProductBrandDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {
        val inputDateActive = p.startDate

        val day = DateFormatUtils.toDateCN(inputDateActive,-1)
        val inputDate = DateFormatUtils.enDateAdd(inputDateActive,-1)

        DataIO.getDataFrameOps.getDimensionDF(
          sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL
        ).registerTempTable("dim_product")

        DataIO.getDataFrameOps.getDF(sc,p.paramMap,LOGINLOG,LogTypes.LOGINLOG).
          select("productModel","mac").
          registerTempTable("log_data")

        DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,inputDate).
          select("openTime","product_model","mac").registerTempTable("log_data1")

        val resultActiveMap = sqlContext.sql(
          "select (case when b.brand_name is null then '其他品牌' else b.brand_name end) as productName, count(distinct mac) " +
            " from log_data a left join dim_product b on a.productModel = b.product_model and b.dim_invalid_time is null" +
            " group by (case when b.brand_name is null then '其他品牌' else b.brand_name end)"
        ).collectAsList().map(row => (row.getString(0), row.getLong(1))).toMap

        //显示未关联的产品型号top
        sqlContext.sql(
          "select a.productModel , count(distinct mac) " +
            " from log_data a left join dim_product b" +
            " on a.productModel = b.product_model and b.dim_invalid_time is null" +
            " where b.brand_name is null" +
            " group by a.productModel" +
            " order by count(distinct mac) desc"
        ).show(30)

        val resultNewMap = sqlContext.sql(
          "select (case when b.brand_name is null then '其他品牌' else b.brand_name end) as productName, count(distinct mac)" +
            " from log_data1 a left join dim_product b on a.product_model = b.product_model and b.dim_invalid_time is null" +
            s" where a.openTime >= '$day 00:00:00' and a.openTime <= '$day 23:59:59'" +
            " group by (case when b.brand_name is null then '其他品牌' else b.brand_name end)"
        ).collectAsList().map(row => (row.getString(0), row.getLong(1))).toMap

        val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
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
