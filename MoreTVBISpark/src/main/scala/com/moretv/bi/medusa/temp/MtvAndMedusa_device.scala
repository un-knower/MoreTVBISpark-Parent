package com.moretv.bi.medusa.temp

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
//1.分终端统计mtv的播放人数及人均时长
//2.分终端统计medusas的播放人数及人均时长
object MtvAndMedusa_device extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val path = "/mbi/parquet/playview/"+date+"/*"
          val path2 = "/log/medusa/parquet/"+date+"/play/*"
          import sqlContext.implicits._
          //载入数据
          val MTV = sqlContext.read.load(path).select("duration","userId","productModel").
            map(row => {
            val device = row.getString(2)
            if(pmfilter(device))
              (row.getInt(0).toLong, row.getString(1),device.toUpperCase)
            else null
          }).filter(_!=null).toDF("duration","userId","productModel").persist()
          val Medusa = sqlContext.read.load(path2).filter("event = 'userexit' or event = 'selfend'").select("duration","userId","productModel").
            map(row => {
            val device = row.getString(2)
            if(pmfilter(device))
              (row.getLong(0), row.getString(1), device.toUpperCase)
            else null
          }).filter(_!=null).toDF("duration","userId","productModel").persist()

          //分终端统计mtv的播放人数及人均时长
          val user_access_mtv = MTV.select("productModel","userId").distinct().groupBy("productModel").count()
          val vv_mtv = MTV.groupBy("productModel").count()
          val sumDur_mtv = MTV.filter("duration < 10800 and duration > 0").select("duration","productModel").groupBy("productModel").sum("duration")
          val user_access_mtv_filtered = MTV.filter("duration < 10800 and duration > 0").select("productModel","userId").distinct().groupBy("productModel").count()
          val res_mtv = sumDur_mtv.join(user_access_mtv_filtered, "productModel").map(row => {
            val device = row.getString(0)
            val sum = row.getLong(1)
            val user_access = row.getLong(2)
            val avgDur = sum.toDouble/user_access
            (device, user_access, avgDur)
          }).collect()

          //分终端统计medusa的播放人数及人均时长
          val user_access_medusa = Medusa.select("productModel","userId").distinct().groupBy("productModel").count()
          val vv_medusa = Medusa.groupBy("productModel").count()
          val sumDur_medusa = Medusa.filter("duration < 10800 and duration > 0").select("duration","productModel").groupBy("productModel").sum("duration")
          val user_access_medusa_filtered = Medusa.filter("duration < 10800 and duration > 0").select("productModel","userId").distinct().groupBy("productModel").count()
          val res_medusa = sumDur_medusa.join(user_access_medusa_filtered,"productModel").map(row => {
            val device = row.getString(0)
            val sum = row.getLong(1)
            val user_access = row.getLong(2)
            val avgDur = sum.toDouble/user_access
            (device, user_access, avgDur)
          }).collect()

          if(p.deleteOld){
            val sqlDeleteAvgDur = "DELETE FROM mtvAndMedusaAvgDur_device WHERE day = ?"
            val sqlDeleteVV = "DELETE FROM mtvAndMedusaVV_device WHERE day = ?"
            util.delete(sqlDeleteAvgDur,day)
            util.delete(sqlDeleteVV,day)
          }
          val sqlInsertAvgDur = "INSERT INTO mtvAndMedusaAvgDur_device(day,tag,device,user_access,avgDur) VALUES(?,?,?,?,?)"
          val sqlInsertVV = "INSERT INTO mtvAndMedusaVV_device(day,tag,device,user_access,vv) VALUES(?,?,?,?,?)"

          res_mtv.foreach(e =>
            util.insert(sqlInsertAvgDur,day,"mtv",e._1,new JLong(e._2),new JDouble(e._3))
          )
          user_access_mtv.join(vv_mtv, "productModel").collect().foreach(row => {
            util.insert(sqlInsertVV,day,"mtv",row.getString(0),new JLong(row.getLong(1)),new JDouble(row.getLong(2)))
          })

          res_medusa.foreach(e =>
            util.insert(sqlInsertAvgDur,day,"medusa",e._1,new JLong(e._2),new JDouble(e._3))
          )
          user_access_medusa.join(vv_medusa, "productModel").collect().foreach(row => {
            util.insert(sqlInsertVV,day,"medusa",row.getString(0),new JLong(row.getLong(1)),new JDouble(row.getLong(2)))
          })
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
  }
  def pmfilter(device:String) = {
    if(device != null && (device.equalsIgnoreCase("we20s") || device.equalsIgnoreCase("M321")|| device.equalsIgnoreCase("LetvNewC1S")||
      device.equalsIgnoreCase("MagicBox_M13")|| device.equalsIgnoreCase("MiBOX3")))
      true
    else false
  }
}

