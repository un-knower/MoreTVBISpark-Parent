package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/4/24.
 */
object DifferentAreaPlayInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("bi")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/mbi/parquet/playview/$dateTime/"

          val df = sqlContext.read.load(inputPath).select("date","event","apkSeries","userId","productModel","path",
            "duration").cache()
          val day = DateFormatUtils.toDateCN(dateTime)
          /*date,event,apkSeries,userId,productModel,path,duration*/
          val logRdd = df.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getString
            (5),e.getInt(6).toLong)).filter(_._1!=null).filter(_._2!=null).filter(_._3!=null).filter(_._4!=null).filter(_
            ._5!=null).filter(_._6!=null).filter(_._7!=null).cache()

          val filterRdd = logRdd.filter(_._3.take(16)=="MoreTV_TVApp2.0_").filter(_._2=="playview").cache()


          /*Getting different area play_num*/
          // The watch history info ...
          val historyRdd = filterRdd.filter(_._6.contains("-")).filter(_._6.split("-").length>=2).filter(_._6.split("-")(1)=="history").cache()
          val history_play_num = historyRdd.map(e=>e._4).count()
          val history_user_num = historyRdd.map(e=>e._4).distinct().count()
          val history_total_duration = historyRdd.map(e=>e._7).filter(e=>{e<14400}).reduce((x,y)=>x+y)
          val insert_history_play_info = "insert into mtv_different_area_play_info(date,area_name,user_num,access_num," +
            "total_duration) values (?,?,?,?,?)"
          util.insert(insert_history_play_info,day,"history",new JLong(history_user_num),new JLong(history_play_num),new
              JLong(history_total_duration))

          // The today recommendation info ...
          val recommendRdd = filterRdd.filter(_._6.contains("-")).filter(_._6.split("-").length>=2).filter(_._6.split("-")(1)=="hotrecommend").cache()
          val recommend_play_num = recommendRdd.map(e=>e._4).count()
          val recommend_user_num = recommendRdd.map(e=>e._4).distinct().count()
          val recommend_total_duration = recommendRdd.map(e=>e._7).filter(e=>{e<14400}).reduce((x,y)=>x+y)
          val insert_recommend_play_info = "insert into mtv_different_area_play_info(date,area_name,user_num,access_num," +
            "total_duration) values (?,?,?,?,?)"
          util.insert(insert_recommend_play_info,day,"recommend",new JLong(recommend_user_num),new JLong(recommend_play_num),new
              JLong(recommend_total_duration))
          // The detail of recommendation info for each recommendation location ...
          val recommend_detail_play_num = recommendRdd.filter(_._6.split("-").length>3).map(e=>(e._6.split("-")(2),e._4))
            .countByKey()
          val recommend_detail_user_num = recommendRdd.filter(_._6.split("-").length>3).map(e=>(e._6.split("-")(2),e._4)).distinct().countByKey()
          val recommend_detail_duration = recommendRdd.filter(_._6.split("-").length>3).map(e=>(e._6.split("-")(2),e._7)).filter(e=>{e._2<14400}).reduceByKey((x,y)=>x+y)
          val locTag = recommend_detail_play_num.keys.toArray
          val recommend_detail_play_num_arr = recommend_detail_play_num.values.toArray
          val recommend_detail_user_num_arr = recommend_detail_user_num.values.toArray
          val recommend_detail_duration_arr = recommend_detail_duration.values.collect()

          val numOfRecommendLoc = recommend_detail_user_num_arr.size
          (0 until numOfRecommendLoc).foreach(i=>{
            val insert_recommend_area_play_info = "insert into mtv_different_area_play_info(date,area_name,user_num," +
              "access_num,total_duration) values (?,?,?,?,?)"
            util.insert(insert_recommend_area_play_info,day,"recommend".concat(locTag(i)),
              new JLong(recommend_detail_user_num_arr(i)),new JLong(recommend_detail_play_num_arr(i)),
              new JLong(recommend_detail_duration_arr(i)))
          })


          // The other watch info ...
          val othersWatchRdd = filterRdd.filter(_._6.contains("-")).filter(_._6.split("-").length>=2).filter(_._6.split("-")(1)=="otherswatch").cache()
          val othersWatch_play_num = othersWatchRdd.map(e=>e._4).count()
          val othersWatch_user_num = othersWatchRdd.map(e=>e._4).distinct().count()
          val othersWatch_total_duration = othersWatchRdd.map(e=>e._7).filter(e=>{e<14400}).reduce((x,y)=>x+y)
          val insert_othersWatch_play_info = "insert into mtv_different_area_play_info(date,area_name,user_num,access_num," +
            "total_duration) values (?,?,?,?,?)"
          util.insert(insert_othersWatch_play_info,day,"othersWatch",new JLong(othersWatch_user_num),new JLong(othersWatch_play_num),new
              JLong(othersWatch_total_duration))


          // The classification info ...
          val classificationArr = Array("tv","movie","mv","zongyi","kids","comic","xiqu","jilu","sport","hot")
          val classificationRdd = filterRdd.filter(_._6.contains("-")).filter(_._6.split("-").length>=2).filter(e=>{classificationArr.contains(e._6.split("-")(1))}).cache()
          val classification_play_num = classificationRdd.map(e=>(e._6.split("-")(1),e._4)).countByKey()
          val classification_user_num = classificationRdd.map(e=>(e._6.split("-")(1),e._4)).distinct().countByKey()
          val classification_duration = classificationRdd.map(e=>(e._6.split("-")(1),e._7)).filter(e=>{e._2<14400})
            .reduceByKey((x,y)=>x+y)
          val classificationTag = classification_play_num.keys.toArray
          val classification_play_num_arr = classification_play_num.values.toArray
          val classification_user_num_arr = classification_user_num.values.toArray
          val classification_duration_arr = classification_duration.values.collect()

          val numOfClassification = classification_play_num_arr.size
          (0 until numOfClassification).foreach(i=>{
            val insert_recommedn_area_play_info = "insert into mtv_different_area_play_info(date,area_name,user_num," +
              "access_num,total_duration) values (?,?,?,?,?)"
            util.insert(insert_recommedn_area_play_info,day,"classification".concat(classificationTag(i)),
              new JLong(classification_user_num_arr(i)),new JLong(classification_play_num_arr(i)),
              new JLong(classification_duration_arr(i)))
          })



          cal.add(Calendar.DAY_OF_MONTH, -1)

          historyRdd.unpersist()
          recommendRdd.unpersist()
          othersWatchRdd.unpersist()
          classificationRdd.unpersist()
          filterRdd.unpersist()
          logRdd.unpersist()
          df.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }



  }
}
