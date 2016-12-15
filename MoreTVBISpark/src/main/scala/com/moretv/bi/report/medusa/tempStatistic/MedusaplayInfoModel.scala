package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/4/24.
 */
object MedusaplayInfoModel extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
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
          /*Getting the dianbo user_num,play_num,average_duration*/
          val total_play_num = filterStartPlayRdd.filter(_._4!=null).map(e=>e._4).count()
          val total_user_num = filterStartPlayRdd.filter(_._4!=null).map(e=>e._4).distinct().count()
          val total_duration = filterRdd.filter(_._2!=null).filter(e=>{e._2=="userexit" || e._2=="selfend"}).filter(_._7!=null).map(e=>e._7)
            .filter(e=>{e<14400 && e>0}).reduce((x,y)=>x+y)
          val insert_all_info = "insert into medusa_different_area_play_info(date,area_name,user_num,access_num," +
            "total_duration) values (?,?,?,?,?)"
          util.insert(insert_all_info,day,"Total",new JLong(total_user_num),new JLong(total_play_num),new JLong
          (total_duration))

          /*Getting different area play_num*/
          // The my_tv info ...
          val myTvArr = Array("history","collect","account","tv","movie","mv","zongyi","kids","comic","xiqu","jilu","sport","hot")
          val myTvRdd = filterRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split("\\*").length>=3)
            .filter(_._6.split("\\*")(1)=="my_tv").filter(_._6.split("\\*")(2).contains("-"))
            .filter(e=>{myTvArr.contains(e._6.split("\\*")(2).split("-")(0))}).persist(StorageLevel.MEMORY_AND_DISK)
          val myTvRddStartPlay = filterStartPlayRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split("\\*")
            .length>=3).filter(_._6.split("\\*")(1)=="my_tv").filter(_._6.split("\\*")(2).contains("-"))
            .filter(e=>{myTvArr.contains(e._6.split("\\*")(2).split("-")(0))}).persist(StorageLevel.MEMORY_AND_DISK)
          val myTv_play_num = myTvRddStartPlay.filter(_._4!=null).map(e=>(e._6.split("\\*")(2).split("-")(0),e._4))
            .countByKey()
          val myTv_user_num = myTvRddStartPlay.filter(_._4!=null).map(e=>(e._6.split("\\*")(2).split("-")(0),e._4))
            .distinct().countByKey()
          val myTv_total_duration = myTvRdd.filter(_._2!=null).filter(e=>{e._2=="userexit" || e._2=="selfend"}).filter(_._7!=null)
            .map(e=>(e._6.split("\\*")(2).split("-")(0),e._7)).filter(e=>{e._2<14400 && e._2>0}).reduceByKey((x,y)=>x+y)


          val kindsArr = myTv_play_num.keys.toArray
          val kindsArr2 = myTv_total_duration.keys.collect()
          val mytvPlayNumArr = myTv_play_num.values.toArray
          val mytvUserNumArr = myTv_user_num.values.toArray
          val mytvTotalDurationArr = myTv_total_duration.values.collect()


          (0 until kindsArr.size).foreach(i=>{
            val insert_myTv_play_info = "insert into medusa_different_area_play_info(date,area_name,user_num,access_num," +
              "total_duration) values (?,?,?,?,?)"
            util.insert(insert_myTv_play_info,day,"mytv".concat(kindsArr(i)),new JLong(mytvPlayNumArr(i)),new JLong
            (mytvUserNumArr(i)),new JLong(mytvTotalDurationArr(i)))
          })
          myTvRdd.unpersist()

          // The today recommendation info ...
          val recommendRdd = filterRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split("\\*").length>=2)
            .filter(_._6.split("\\*")(1)=="recommendation").persist(StorageLevel.MEMORY_AND_DISK)
          val recommendRddStartPlay = filterStartPlayRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split
            ("\\*").length>=2).filter(_._6.split("\\*")(1)=="recommendation").persist(StorageLevel.MEMORY_AND_DISK)
          val recommend_play_num = recommendRddStartPlay.filter(_._4!=null).map(e=>e._4).count()
          val recommend_user_num = recommendRddStartPlay.filter(_._2!=null).filter(_._4!=null).map(e=>e._4).distinct().count()
          val recommend_total_duration = recommendRdd.filter(_._2!=null).filter(e=>{e._2=="userexit" || e._2=="selfend"})
            .filter(_._7!=null).map(e=>e._7).filter(e=>{e<14400 && e>0}).reduce((x,y)=>x+y)
          val insert_recommend_play_info = "insert into medusa_different_area_play_info(date,area_name,user_num,access_num," +
            "total_duration) values (?,?,?,?,?)"
          util.insert(insert_recommend_play_info,day,"recommend",new JLong(recommend_user_num),new JLong(recommend_play_num),new
              JLong(recommend_total_duration))
          recommendRdd.unpersist()
          recommendRddStartPlay.unpersist()

          // The foundation info ...
          val foundationRdd = filterRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split("\\*").length>=2)
            .filter(_._6.split("\\*")(1)=="foundation").persist(StorageLevel.MEMORY_AND_DISK)
          val foundationRddStartPlay = filterStartPlayRdd.filter(_._6!=null).filter(_._6.contains("*")).filter(_._6.split
            ("\\*").length>=2).filter(_._6.split("\\*")(1)=="foundation").persist(StorageLevel.MEMORY_AND_DISK)
          val foundation_play_num = foundationRddStartPlay.filter(_._4!=null).map(e=>e._4).count()
          val foundation_user_num = foundationRddStartPlay.filter(_._4!=null).map(e=>e._4).distinct().count()
          val foundation_total_duration = foundationRdd.filter(_._2!=null).filter(e=>{e._2=="userexit" || e._2=="selfend"})
            .filter(_._7!=null).map(e=>e._7).filter(e=>{e<14400 && e>0}).reduce((x,y)=>x+y)
          val insert_foundation_play_info = "insert into medusa_different_area_play_info(date,area_name,user_num,access_num," +
            "total_duration) values (?,?,?,?,?)"
          util.insert(insert_foundation_play_info,day,"foundation",new JLong(foundation_user_num),new JLong(foundation_play_num)
            ,new JLong(foundation_total_duration))
          foundationRdd.unpersist()


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


          // The play user_num by productModel
          val filterProductModelArr = Array("LetvNewC1S","we20s","M321","MagicBox_M13","MiBOX3")
          val playNumByProduct = filterStartPlayRdd.filter(_._2=="startplay").map(e=>(e._5,e._4)).filter(e=>{filterProductModelArr.contains(e._1)}).countByKey()
          val userNumByProduct = filterStartPlayRdd.filter(_._2=="startplay").map(e=>(e._5,e._4)).filter(e=>{filterProductModelArr.contains(e._1)}).distinct().countByKey()
          val productModelArr = playNumByProduct.keys.toArray
          val playNumByProductArr = playNumByProduct.values.toArray
          val userNumByProductArr = userNumByProduct.values.toArray
          val numOfProduct = userNumByProductArr.size
          (0 until numOfProduct).foreach(i=>{
            val insertPlayUserNumSql = "insert into medusa_by_product_play_info(date,product_model,user_num,play_num) " +
              "values " +
              "(?,?,?,?)"
            util.insert(insertPlayUserNumSql,day,productModelArr(i),new JLong(userNumByProductArr(i)),new JLong
            (playNumByProductArr(i)))
          })

          cal.add(Calendar.DAY_OF_MONTH, -1)
          filterStartPlayRdd.unpersist()
          filterRdd.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }

  }
}
