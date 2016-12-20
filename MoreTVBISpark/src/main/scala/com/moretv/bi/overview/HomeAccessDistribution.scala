package com.moretv.bi.overview

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object HomeAccessDistribution extends BaseClass with DateUtil{

  def main(args: Array[String]) {
    config.setAppName("HomeAccessDistribution")
    ModuleClass.executor(HomeAccessDistribution,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/homeaccess/"+p.startDate
        val df = sqlContext.read.load(path).filter("event = 'enter'").
          select("date","accessArea","accessLocation","userId").
          map(e =>(e.getString(0),e.getString(1),e.getString(2),e.getString(3))).persist(StorageLevel.MEMORY_AND_DISK)

        val firstRDD = df.map(e =>(getKeys(e._1,e._2,e._3,true),e._4)).filter(_ != null)
        val secondRDD = df.filter(e => e._2.toInt<5).map(e =>(getKeys(e._1,"6",e._3,true),e._4)).filter(_!=null)
        val thirdRDD = df.map(e =>(getKeys(e._1,"0",e._3,true),e._4)).filter(_!=null)
        val fourRDD = df.filter(e => e._2.toInt == 1).map(e =>(getKeys(e._1,e._2,e._3,false),e._4)).filter(_!=null)

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from home_access_distribution where day = '$date'"
          db.delete(oldSql)
        }
        //insert new data
        saveRdd2DB(firstRDD,db)
        saveRdd2DB(secondRDD,db)
        saveRdd2DB(thirdRDD,db)
        saveRdd2DB(fourRDD,db)
        df.unpersist()
        db.destory()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, accessArea:String, accessLocation:String, flag:Boolean)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    val index =
      if(flag){
        accessArea
      }else{
        if(accessLocation.toInt <= 4) "101" else if(accessLocation.toInt <= 9) "102" else "103"
      }
    val name = getNameByIndex(index)
    if(name != null) (year,month,date,index,name) else null
  }

  def getNameByIndex(index:String)={
    val names = Map(
      "0"->"首页总计",
      "1"->"热门推荐",
      "2"->"卫视直播",
      "3"->"大家在看",
      "4"->"观看历史",
      "5"->"分类入口",
      "6"->"节目类",
      "101"->"热门推荐第一屏",
      "102"->"热门推荐第二屏",
      "103"->"热门推荐第三屏"
    )
    names.getOrElse(index,null)
  }

  def saveRdd2DB(rdd:RDD[((String,Int,String,String,String),String)],db: MySqlOps) = {
    val userNum = rdd.distinct().countByKey()
    val accessNum = rdd.countByKey()
    val sql = "INSERT INTO home_access_distribution(year,month,day,region_code,region_name,user_num,access_num) " +
      "VALUES(?,?,?,?,?,?,?)"
    userNum.foreach(x =>{
      db.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),
        new Integer(accessNum(x._1).toInt))
    })
  }
}
