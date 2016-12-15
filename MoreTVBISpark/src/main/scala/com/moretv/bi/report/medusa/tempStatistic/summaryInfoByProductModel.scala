package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/4/24.
 */
object summaryInfoByProductModel extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/moretvloginlog/parquet/$dateTime/loginlog"

          val df = sqlContext.read.load(inputPath).cache()
          val dfNew = df.select("mac","productModel","userId","version")
          val day = DateFormatUtils.toDateCN(dateTime)
          /*mac,productModel,userId,version*/
          val logRdd = dfNew.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3)))
          val filterRdd = logRdd.filter(_._1!=null).filter(_._2!=null).filter(_._3!=null).filter(_._4!=null).filter(_._4.take
            (16) =="MoreTV_TVApp2.0_").filter(e=>{e._2=="LetvNewC1S" || e._2=="we20s" ||e._2=="M321" || e._2=="MagicBox_M13" || e._2=="MiBOX3"})
          /*Getting live play_num and aver_duration*/
          val activeLoginNumByProduct = filterRdd.map(e=>(e._2,e._3)).countByKey()
          val activeUserNumByProduct = filterRdd.map(e=>(e._2,e._3)).distinct().countByKey()
          val productModelArr = activeLoginNumByProduct.keys.toArray
          val activeUserNumByProductArr = activeUserNumByProduct.values.toArray
          val activeLoginNumByProductArr = activeLoginNumByProduct.values.toArray
          val numOfProductModel = activeLoginNumByProductArr.size
          (0 until numOfProductModel).foreach(i=>{
            val insertActiveUserSql = "insert into mtv_active_user_by_product(date,product,login_num,user_num) values (?," +
              "?,?,?)"
            util.insert(insertActiveUserSql,day,productModelArr(i),new JLong(activeLoginNumByProductArr(i)),new JLong
            (activeUserNumByProductArr(i)))
          })





          cal.add(Calendar.DAY_OF_MONTH, -1)
          logRdd.unpersist()
          df.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }



  }
}
