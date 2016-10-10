package com.moretv.bi.login

import com.moretv.bi.constant.Database
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.{Long => JLong}

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * 创建人：连凯
  * 创建时间：2016-04-16
  * 程序作用：统计登录用户的终端型号和终端品牌分布
  * 输入数据为：loginlog，输出到mysql
  *
  */
object ProductModelDist extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(ProductModelDist,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"

        val logRdd = sqlContext.read.load(inputPath).select("productModel","mac").
          map(row => if(row.getString(0) == null){
            (("null","null"),row.getString(1))
          } else {
            val productModel = row.getString(0)
            val productBrand  = ProductModelUtils.getBrand(productModel)
            ((productModel,productBrand),row.getString(1))
          }).cache()
        val loginMap = logRdd.countByKey()
        val userMap = logRdd.distinct().countByKey()

        val db = new DBOperationUtils(Database.EAGLE_TV)
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld){
          val sqlDelete = "delete from Device_Terminal_login where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into Device_Terminal_login(day,product_model,product_brand,user_num,login_num) values(?,?,?,?,?)"
        userMap.foreach(x => {
          val key = x._1
          val userNum = x._2
          val loginNum = loginMap(key)
          val (productModel,productBrand) = key
          db.insert(sqlInsert,day,productModel,productBrand,new JLong(userNum),new JLong(loginNum))
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
