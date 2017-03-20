package com.moretv.bi.temp

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by liankai on 2016/5/16.
  *
  */
object EnterNum4AD extends BaseClass{

  val coreCities = List("北京","上海","广州")
  val aCities = List("深圳","沈阳","南京","成都")
  val bCities = List("大连","长沙","武汉","青岛","杭州","天津","重庆","西安","苏州","宁波","合肥","济南","昆明")

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        sqlContext.udf.register("getCityType",(city:String) => {
          if(city != null){
            if(coreCities.contains(city)) "核心城市"
            else if(aCities.contains(city)) "重点城市A"
            else if(bCities.contains(city)) "重点城市B"
            else "其他"
          }else "其他"
        })
        sqlContext.udf.register("getCity",(city:String) => {
          if(city != null){
            if(coreCities.contains(city) || aCities.contains(city) || bCities.contains(city)) city
            else "剩余城市"
          }else "剩余城市"
        })
        sqlContext.read.load("/log/medusa/userAreaInfo/20170309/userArea").
          selectExpr("user_id","getCity(city) as city","getCityType(city) as cityType").
          persist(StorageLevel.MEMORY_AND_DISK).
          registerTempTable("user_area")

        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.ENTER,date).
            select("userId")
            .registerTempTable("play_data")



          sqlContext.sql("select a.*,b.city,b.cityType from play_data a join user_area b on a.userId = b.user_id").
            registerTempTable("log_data")

          val result = sqlContext.sql("select city,cityType,count(userId) as play_num" +
            " from log_data group by city,cityType").collect()



          if(p.deleteOld){
            util.delete("delete from temp_tencent_enter_num where day = ?",day)
          }
          val insertSql="insert into temp_tencent_enter_num(day,city,city_type,play_num) " +
            "values (?,?,?,?)"

          result.foreach(row => {
            println(row)
            util.insert(insertSql,day,row.get(0),row.get(1),row.get(2))
          })



        })
        util.destory()
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
