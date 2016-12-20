package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.DriverManager
import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, DBOperationUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by xia jun on 2016/9/5.
 * @author xia jun
 * Computing the live duration of each time period
  *
  * need to delete,not used on prod
 */
object LiveDurationAnalytics extends BaseClass{
  val secondInterval = 10
  val periodNum = 8640

  /**
   * @param args
   */
  def main(args: Array[String]) {
    ModuleClass.executor(LiveDurationAnalytics,args)             //用于调用运行框架
  }

  /**
   * function body
   * @param args
   */
  override def execute(args:Array[String]) ={
    val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    val url = db.prop.getProperty("url")
    val driver = db.prop.getProperty("driver")
    val user = db.prop.getProperty("user")
    val password = db.prop.getProperty("password")

    val (min,max) = db.queryMaxMinID("mtv_account","id")
    db.destory()

    val sqlInfo ="select time,probability from medusa.liveDurationProbabilityByTenSecends where id >=? and id <= ?"
    val probabilityRdd = MySqlOps.
      getJdbcRDD(sc,sqlInfo,"liveDurationProbabilityByTenSecends",r=>(r.getString(1),r.getDouble(2)),driver,url,user,password,(min,max),20)



    val tempSqlContext = sqlContext
    import tempSqlContext.implicits._
    /*val numOfPartition = 20
    val minId = (util: MySqlOps) => {
      val sql = "select min(id) from medusa.liveDurationProbabilityByTenSecends"
      val id = util.selectOne(sql)
      id(0).toString.toLong
    }
    val maxId = (util: MySqlOps) => {
      val sql = "select max(id) from medusa.liveDurationProbabilityByTenSecends"
      val id = util.selectOne(sql)
      id(0).toString.toLong
    }
    val probabilityRdd = new JdbcRDD(
      sc,
      ()=>{
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://10.10.2" +
          ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
      },
     "select time,probability from medusa.liveDurationProbabilityByTenSecends where id >=? and id <= ?",
      minId(db),
      maxId(db),
      numOfPartition,
      r => (r.getString(1),r.getDouble(2))
    )
*/
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val dir = s"/log/medusaAndMoretvMerger/$date/live"
          val df = tempSqlContext.read.parquet(dir).select("duration","liveType","userId").filter("liveType = 'live'").
            filter("duration >=0 and duration <=86400")
          df.map(row =>(row.getLong(0),row.getString(2))).map(e=>time2Period(e._1,e._2)).
          toDF("period","index","duration","userId").registerTempTable("log")
          val rdd = tempSqlContext.sql("select period,index,sum(duration)/count(distinct userId) from log " +
            "group by period,index").map(e=>(e.getString(0),(e.getInt(1),e.getDouble(2))))
          val durationEM = (probabilityRdd.join(rdd)).map(e=>(e._1,e._2._2._1,e._2._1*e._2._2._2)).persist(StorageLevel.MEMORY_AND_DISK)
          val tenS2tenM = durationEM.filter(e=>{e._2>=1 && e._2<60}).map(e=>e._3).sum()
          val tenM2ThirtyM = durationEM.filter(e=>{e._2>=60 && e._2<180}).map(e=>e._3).sum()
          val thirtyM2OneH = durationEM.filter(e=>{e._2>=180 && e._2<360}).map(e=>e._3).sum()
          val oneH2TwoH = durationEM.filter(e=>{e._2>=360 && e._2<720}).map(e=>e._3).sum()
          val oneH2FourH = durationEM.filter(e=>{e._2>=720 && e._2<1440}).map(e=>e._3).sum()
          val fourH2SixH = durationEM.filter(e=>{e._2>=1440 && e._2<2160}).map(e=>e._3).sum()
          val sixH2EightH = durationEM.filter(e=>{e._2>=2160 && e._2<2880}).map(e=>e._3).sum()
          val eightH2TwelveH = durationEM.filter(e=>{e._2>=720 && e._2<4320}).map(e=>e._3).sum()
          val twelveH2FinalH = durationEM.filter(e=>{e._2>=4320 && e._2<8640}).map(e=>e._3).sum()
          println(tenS2tenM)
          println(tenM2ThirtyM)
          println(thirtyM2OneH)
          println(oneH2TwoH)
          println(oneH2FourH)
          println(fourH2SixH)
          println(sixH2EightH)
          println(eightH2TwelveH)
          println(twelveH2FinalH)




        })
      }
      case None => {throw new RuntimeException("At least need one paramter: startDate!")}
    }
  }

  /**
   * Map the duration to period
   * @param duration
   * @param userId
   * @return
   */
  def time2Period(duration:Long,userId:String) = {
    var res = ("default",0,0L,"")
    (1 until periodNum+1).foreach(i=>{
      val upperLimit = secondInterval*i
      val lowerLimit = secondInterval*(i-1)
      if(duration>lowerLimit && duration<=upperLimit){
        res = (lowerLimit.toString.concat("~").concat(upperLimit.toString),i,duration,userId)
      }
    })
    res
  }





}
