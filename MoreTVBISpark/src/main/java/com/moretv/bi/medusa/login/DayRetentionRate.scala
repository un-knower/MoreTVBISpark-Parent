package com.moretv.bi.medusa.login

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{ParamsParseUtil, SparkSetting, UserIdUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object DayRetentionRate extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("DayRetentionRate")
    ModuleClass.executor(DayRetentionRate,args)
  }

  override def execute(args: Array[String]) {

    val numOfPartition = 40
    val needToCalc = Array(1,2,5)
    val numOfDay = Array("one","three","seven")

    ParamsParseUtil.parse(args) match{
      case Some(p) =>{
        //Run one day data default
        val numOfDaysToCalc =p.numOfDays
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        val date = readFormat.parse(p.startDate)
        //obtain Calendar object
        val calendar = Calendar.getInstance()
        calendar.setTime(date)
        val product_models = List("we20s","M321" ,"LetvNewC1S" ,"MagicBox_M13" ,"MiBOX3")
        //calculate data
        for(i<-0 until numOfDaysToCalc){
          val c = Calendar.getInstance()
          c.set(calendar.get(Calendar.YEAR),calendar.get(Calendar.MONTH),calendar.get(Calendar.DATE)-1)
          //run parquet
          val path = "/log/moretvloginlog/parquet/"+readFormat.format(calendar.getTime)+"/loginlog/part-r-*"
          val all_parquet = sqlContext.read.parquet(path).select("mac","productModel").map(e=>(e.getString(0),e.getString(1)))
          val logUserID = all_parquet.filter(e=>filterProductModel(product_models,e._2)).map(e =>UserIdUtils.userId2Long(e._1)).filter(_!=null).distinct().persist(StorageLevel.MEMORY_AND_DISK)

          calendar.add(Calendar.DAY_OF_MONTH,1)
          //connect database
          Class.forName("com.mysql.jdbc.Driver")
          val connection = DriverManager.getConnection(s"jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
          val stmt = connection.createStatement()

          for(j<- 0 until needToCalc.length){
            c.add(Calendar.DAY_OF_MONTH,-needToCalc(j))
            val date2 = format.format(c.getTime)
            val id = getID(date2,stmt)
            val min = id(0)
            val max =id(1)
            val sqlRDD = new JdbcRDD(sc, ()=>{
              Class.forName("com.mysql.jdbc.Driver")
              DriverManager.getConnection(s"jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
                "bi", "mlw321@moretv")
            },
              "SELECT mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? and product_model in ('we20s','M321' ,'LetvNewC1S' ,'MagicBox_M13' ,'MiBOX3') and mac is not null and left(openTime,10) = '" + date2 + "'",
              min,
              max,
              numOfPartition,
              r=>r.getString(1)).map(UserIdUtils.userId2Long).distinct().persist(StorageLevel.MEMORY_AND_DISK)

            val retention = logUserID.intersection(sqlRDD).count()
            val newUser = sqlRDD.count().toInt
            val retentionRate = retention.toDouble/newUser.toDouble
            if(j==0){
              insertSQL(date2,newUser,retentionRate,stmt)
            }else{
              updateSQL(numOfDay(j),retentionRate,date2,stmt)
            }
            sqlRDD.unpersist()
          }
          logUserID.unpersist()
        }
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

  def filterProductModel(product_models:List[String],productModel:String): Boolean ={
    var flag = false
    product_models.foreach(x=>{
      if(x.equalsIgnoreCase(productModel))
        flag = true
    })
    flag
  }

  def getID(day: String, stmt: Statement): Array[Long]={
    val sql = s"SELECT MIN(id),MAX(id) FROM `mtv_account` WHERE LEFT(openTime, 10) = '$day'"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1),id.getLong(2))
  }

  def insertSQL(date: String, count: Int, retention: Double, stmt: Statement) ={
    val sql = s"INSERT INTO medusa.`user_retetion_day` (DAY, new_user_num, ONE) VALUES('$date', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num:String, retention:Double, date:String, stmt: Statement)={
    val sql = s"UPDATE medusa.`user_retetion_day` SET $num = $retention WHERE DAY = '$date'"
    stmt.executeUpdate(sql)
  }
}