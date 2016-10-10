package com.moretv.bi.report

import java.io.PrintWriter

import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, ProductModelUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * 创建人：连凯
  * 创建时间：2015/5/8.
  * 程序作用：用于统计当前数据库中用户的终端型号和终端品牌分布
  * 数据输入：登录日志
  * 数据输出：登录用户的终端型号和终端品牌分布
 */
object LoginUserProductModelDist extends SparkSetting{

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val inputDate = p.startDate
        sqlContext.read.load(s"/log/moretvloginlog/parquet/$inputDate/loginlog").registerTempTable("log_data")
        val logRdd = sqlContext.sql("select productModel,mac from log_data").
          map(row => (row.getString(0),row.getString(1))).distinct().cache()


        //将数据结果写入文件，为了支持多天输入，所以文件名暂时为固定的
        val modelMap = logRdd.countByKey()
//        val day = DateFormatUtils.toDateCN(inputDate,-1)
        val file = s"/script/bi/moretv/liankai/file/ModelDist.csv"
        val out = new PrintWriter(file,"GBK")
        modelMap.foreach(
          e =>
            out.println(e._1+","+e._2)
        )
        out.close()

        val brandMap = logRdd.map(x => (ProductModelUtils.getBrand(x._1),x._2)).distinct().countByKey()
        val file2 = s"/script/bi/moretv/liankai/file/BrandDist.csv"
        val out2 = new PrintWriter(file2,"GBK")
        brandMap.foreach(e => out2.println(e._1+","+e._2))
        out2.close()
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}
