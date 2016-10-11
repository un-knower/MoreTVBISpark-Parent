package com.moretv.bi.report.medusa.tempStatistic

import java.util.Calendar
import java.lang.{Long=>JLong}
import com.moretv.bi.report.medusa.tempStatistic.MedusaplayInfoModel._
import com.moretv.bi.util.{DateFormatUtils, DBOperationUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/4/26.
 */
object MedusaClassificationPlayInfo {
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("bi")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/medusa/parquet/$dateTime/play"
          val day = DateFormatUtils.toDateCN(dateTime)
          sqlContext.read.load(inputPath).registerTempTable("log")
          val df = sqlContext.sql("select date,event,apkSeries,userId,productModel,pathMain,duration from log where " +
            "event in ('userexit','selfend')")
          val dfStartPlay = sqlContext.sql("select date,event,apkSeries,userId,productModel,pathMain from log where " +
            "event = 'startplay'")

          /*date,event,apkSeries,userId,productModel,path,duration*/
          val logRdd = df.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getString
            (5),e.getLong(6)))
          val logRddStartPlay = dfStartPlay.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString
            (4),e.getString(5)))

          val filterRdd = logRdd.filter(_._3!=null).filter(_._3.take(16)=="MoreTV_TVApp3.0_").persist(StorageLevel
            .MEMORY_AND_DISK)
          val filterStartPlayRdd = logRddStartPlay.filter(_._3!=null).filter(_._3.take(16)=="MoreTV_TVApp3.0_").
            persist(StorageLevel.MEMORY_AND_DISK)

          // The classification info ...
          val classificationArr = Array("tv","movie","mv","zongyi","kids","comic","xiqu","jilu","sport","hot","application")
          val classificationRdd = filterRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split("\\*")
            .length>=3).filter(_._6.split("\\*")(1)=="classification").filter(_._6.split("\\*")(2).contains("-"))
            .filter(e=>{classificationArr.contains(e._6.split("\\*")(2).split("-")(0))}).persist(StorageLevel
            .MEMORY_AND_DISK)
          val classificationRddStartPlay = filterStartPlayRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6
            .split("\\*").length>=3).filter(_._6.split("\\*")(1)=="classification").filter(_._6.split("\\*")(2).contains
            ("-")).filter(e=>{classificationArr.contains(e._6.split("\\*")(2).split("-")(0))}).persist(StorageLevel
            .MEMORY_AND_DISK)
          val classification_play_num = classificationRddStartPlay.filter(_._4!=null).map(e=>(e._6.split("\\*")(2).split
            ("-")(0),e._4)).countByKey()
          val classification_user_num = classificationRddStartPlay.filter(_._4!=null).map(e=>(e._6.split("\\*")(2).split
            ("-")(0),e._4)).distinct().countByKey()
          val classification_duration = classificationRdd.filter(_._2!=null).filter(e=>{e._2=="userexit" || e._2=="selfend"})
            .filter(_._7!=null).map(e=>(e._6.split("\\*")(2).split("-")(0),e._7)).filter(e=>{e._2<14400 && e._2>0})
            .reduceByKey((x,y)=>x+y)
          val classificationTag = classification_play_num.keys.toArray
          val classification_play_num_arr = classification_play_num.values.toArray
          val classification_user_num_arr = classification_user_num.values.toArray
          val classification_duration_arr = classification_duration.values.collect()
          val numOfClassification = classification_play_num_arr.size
          (0 until numOfClassification).foreach(i=>{
            val insert_recommedn_area_play_info = "insert into medusa_different_area_play_info(date,area_name,user_num," +
              "access_num,total_duration) values (?,?,?,?,?)"
            util.insert(insert_recommedn_area_play_info,day,"classification".concat(classificationTag(i)),
              new JLong(classification_user_num_arr(i)),new JLong(classification_play_num_arr(i)),
              new JLong(classification_duration_arr(i)))
          })
          classificationRdd.unpersist()
          classificationRddStartPlay.unpersist()

          cal.add(Calendar.DAY_OF_MONTH, -1)
          filterStartPlayRdd.unpersist()
          filterRdd.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }

  }

}
