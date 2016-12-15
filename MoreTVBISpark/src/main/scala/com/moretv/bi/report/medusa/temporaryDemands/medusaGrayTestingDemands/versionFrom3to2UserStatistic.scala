package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar
import java.lang.{Long=>JLong}
import com.moretv.bi.util.{DateFormatUtils, DBOperationUtils, SparkSetting, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by xiajun on 2016/5/12.
 * 该对象用于统计重新安装2.X的用户的数量
 */
object versionFrom3to2UserStatistic extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val logType = "enter"
        val medusaFileDir = "/log/medusa/parquet/"
        val moretvFileDir = "/mbi/parquet/"

        /**
         * 统计重新安装2.X的用户数
         */
        val sqlSparkMedusa = "select productModel,userId,max(datetime) from log_data_medusa where event='enter' and " +
          "apkVersion!=null group by userId,productModel"
        val sqlSparkMoretv = "select productModel,userId,max(datetime) from log_data_moretv where logType='enter' and " +
          "apkVersion!=null group by userId,productModel"
         val sqlInsert = "insert into medusa_transform_from_3_to_2_user_by_product(day,productModel," +
          "user_num) values (?,?,?)"
        val sqlDelete = "delete from medusa_transform_from_3_to_2_user_by_product where day = ?"
        val startDate = p.startDate
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val day = DateFormatUtils.readFormat.format(cal.getTime)
          val date = DateFormatUtils.toDateCN(day,-1)


          // 获取每天点击的人数与次数
          // 从parquet中获取数据
          val medusaLogData = sqlContext.read.parquet(s"$medusaFileDir$day/$logType").persist(StorageLevel.DISK_ONLY)
          val moretvLogData = sqlContext.read.parquet(s"$moretvFileDir$logType/$day").persist(StorageLevel.DISK_ONLY)
          medusaLogData.select("apkVersion","productModel","datetime","userId","event").
            registerTempTable("log_data_medusa")
          moretvLogData.select("apkVersion","productModel","datetime","userId","logType").
            registerTempTable("log_data_moretv")
          val medusaDf = sqlContext.sql(sqlSparkMedusa)
          val moretvDf = sqlContext.sql(sqlSparkMoretv)
          val medusaRdd = medusaDf.map(e=>(e.getString(0),e.getString(1),e.getString(2)))
          val moretvRdd = moretvDf.map(e=>(e.getString(0),e.getString(1),e.getString(2)))

          val conUserRdd = medusaRdd.map(e=>((e._1,e._2),e._3)) join(moretvRdd.map(e=>((e._1,e._2),e._3)))
          val mergerRdd = conUserRdd.map(e=>(e._1._1,e._1._2,e._2._1,e._2._2))
          val filterRdd = mergerRdd.filter(e=>{e._4>e._3}).map(e=>(e._1,e._2)).countByKey()

          // 删除数据
          if(p.deleteOld){
            util.delete(sqlDelete,date)
          }

          // 插入数据
          filterRdd.foreach(e=>{
            util.insert(sqlInsert,date,e._1,new JLong(e._2))
          })

          if(filterRdd.size>1){
            val sumInfo = filterRdd.map(e=>e._2).reduce((x,y)=>x+y)
            util.insert(sqlInsert,date,"All",new JLong(sumInfo))
          }else{
            util.insert(sqlInsert,date,"All",new JLong(0))
          }


          cal.add(Calendar.DAY_OF_MONTH,-1)
          medusaLogData.unpersist()
          moretvLogData.unpersist()
        })



      }
      case None => {throw new RuntimeException("At least needs on param: startDate!")}
    }
  }
}
