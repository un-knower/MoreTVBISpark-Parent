package com.moretv.bi.medusa.temp

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

//1.mtv和medusa总计的播放人数及人均时长
//2.分终端统计mtv和medusa总计的播放人数及人均时长
//3.mtv和medusa每日分别的播放人数次数和人均时长
object MtvAndMedusaTotalVV extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val pathMTV = "/mbi/parquet/playview/"+date
          val pathMDS = "/log/medusa/parquet/"+date+"/play"
          import sqlContext.implicits._
          //读入数据
          val MTV = sqlContext.read.load(pathMTV).select("duration","userId","productModel").
            map(row => {
            val device = row.getString(2)
            if(pmfilter(device))
              (row.getInt(0).toLong, row.getString(1),device.toUpperCase)
            else null
          }).filter(_!=null).toDF("duration","userId","productModel").persist()
          val Medusa = sqlContext.read.load(pathMDS).filter("event = 'userexit' or event = 'selfend'").select("duration","userId","productModel").
            map(row => {
            val device = row.getString(2)
            if(pmfilter(device))
              (row.getLong(0), row.getString(1), device.toUpperCase)
            else null
          }).filter(_!=null).toDF("duration","userId","productModel").persist()

          //mtv和medusa总计的播放人数及人均时长
          val Total = MTV.unionAll(Medusa)
          val user_access = Total.select("userId").distinct().count()
          val vv = Total.count()
          val avgDur = Total.select("duration","userId").
            filter("duration < 10800 and duration > 0").selectExpr("sum(duration)/count(distinct userId)").first().getDouble(0)

          //分终端统计mtv和medusa总计的播放人数及人均时长
          val user_access_device = Total.select("productModel","userId").distinct().groupBy("productModel").count()
          val vv_device = Total.groupBy("productModel").count()
          val sumDur_device = Total.select("productModel","duration").filter("duration < 10800 and duration > 0").
            groupBy("productModel").sum("duration")
          val res = sumDur_device.join(user_access_device,"productModel").map(row => {
            val device = row.getString(0)
            val sum = row.getLong(1)
            val user_access = row.getLong(2)
            val avgDur = sum.toDouble/user_access
            (device, user_access, avgDur)
          }).collect()

          //mtv总的播放人数及人均时长
          val user_access_mtv = MTV.select("userId").distinct().count()
          val vv_mtv = MTV.count()
          val avgDur_mtv = MTV.select("duration","userId").filter("duration < 10800 and duration > 0").
            selectExpr("sum(duration)/count(distinct userId)").first().getDouble(0)

          //medusa总的播放人数及人均时长
          val user_access_medusa = Medusa.select("userId").distinct().count()
          val vv_medusa = Medusa.count()
          val avgDur_medusa = Medusa.select("duration","userId").filter("duration < 10800 and duration > 0").
            selectExpr("sum(duration)/count(distinct userId)").first().getDouble(0)

          if(p.deleteOld){
            val sqlDelete1 = "DELETE FROM mtvAndMedusaTotalVV WHERE day = ?"
            val sqlDelete2 = "DELETE FROM mtvAndMedusaTotalAvgDur_device WHERE day = ?"
            val sqlDelete3 = "DELETE FROM mtvAndMedusaTotalVV_device WHERE day = ?"
            val sqlDelete4 = "DELETE FROM mtvAndMedusaVV WHERE day = ?"
            util.delete(sqlDelete1,day)
            util.delete(sqlDelete2,day)
            util.delete(sqlDelete3,day)
            util.delete(sqlDelete4,day)
          }
          val sqlInsertTotal = "INSERT INTO mtvAndMedusaTotalVV(day,user_access,vv,avgDur) VALUES(?,?,?,?)"
          util.insert(sqlInsertTotal,day,new JLong(user_access), new JLong(vv),new JDouble(avgDur))

          val sqlInsert_device = "INSERT INTO mtvAndMedusaTotalAvgDur_device(day,device,user_access,avgDur) VALUES(?,?,?,?)"
          res.foreach(e =>{
            util.insert(sqlInsert_device,day, e._1,new JLong(e._2), new JDouble(e._3))
          })

          val sqlInsert_vv = "INSERT INTO mtvAndMedusaTotalVV_device(day,device,user_access,vv) VALUES(?,?,?,?)"
          user_access_device.join(vv_device, "productModel").collect().foreach(row => {
            util.insert(sqlInsert_vv,day,row.getString(0),new JLong(row.getLong(1)),new JDouble(row.getLong(2)))
          })

          val sqlInsert = "INSERT INTO mtvAndMedusaVV(day,tag,user_access,vv,avgDur) VALUES(?,?,?,?,?)"
          util.insert(sqlInsert,day,"moretv",new JLong(user_access_mtv), new JLong(vv_mtv),new JDouble(avgDur_mtv))
          util.insert(sqlInsert,day,"medusa",new JLong(user_access_medusa), new JLong(vv_medusa),new JDouble(avgDur_medusa))

          cal.add(Calendar.DAY_OF_MONTH, -1)
          MTV.unpersist()
          Medusa.unpersist()
        })
        util.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
    def pmfilter(device:String) = {
      if(device != null && (device.equalsIgnoreCase("we20s") || device.equalsIgnoreCase("M321")|| device.equalsIgnoreCase("LetvNewC1S")||
        device.equalsIgnoreCase("MagicBox_M13")|| device.equalsIgnoreCase("MiBOX3")))
        true
      else false
    }
  }
}

