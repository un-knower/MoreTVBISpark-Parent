package com.moretv.bi.report.medusa.util
import scala.collection.mutable.Map
import java.sql.{ResultSet, Statement, DriverManager, Connection}

/**
 * Created by chubby on 2016/9/2.
 *
 * Getting data from database
 */
object DataFromDB {
  /**
   * Define the data variable
   */
  var tencentCid2SidMap = Map[String,String]()

  /**
   * Define the database connection info
   */
  lazy val driver:String = "com.mysql.jdbc.Driver"
  lazy val userName_15:String = "bi"
  lazy val password_15:String = "mlw321@moretv"
  lazy val url_mtv_bi_15:String = "jdbc:mtsql://10.10.2.15:3306/medusa?userUnicode=true&characterEncoding=utf-8&autoReconnect=true"
  lazy val userName_23:String = "bislave"
  lazy val password_23:String = "slave4bi@whaley"
  lazy val url_mtv_cms_23:String = "jdbc:mysql://10.10.2.23:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"

  /**
   * SQL Info
   */
  val tencentCid2SidSql:String = "select qqid, baseContentSid from mtv_tencent_base where qqid is not null"
  val liveDurationProbabilitySql:String = "select time, probability from liveDurationProbabilityByTenSecends"
  /**
   * Initialize the database connection and query data
   * @param url
   * @param sql
   * @param map
   * @param userName
   * @param password
   */
  def initMap(url:String,sql:String,map:Map[String,String],userName:String,password:String) = {
    try{
      Class.forName(driver)
      val conn:Connection = DriverManager.getConnection(url,userName,password)
      val stat:Statement = conn.createStatement()
      val rs:ResultSet = stat.executeQuery(sql)
      while (rs.next()){
        map += (rs.getString(1) -> rs.getString(2))
      }
      rs.close()
      stat.close()
      conn.close()
    }catch {
      case e:Exception =>{
        throw new RuntimeException(e)
      }
    }
  }

  /**
   * Getting sid from tencent video cid
   * @param cid
   */
  def getTencentCid2Sid(cid:String) = {
    if(tencentCid2SidMap.isEmpty){
      initMap(url_mtv_cms_23,tencentCid2SidSql,tencentCid2SidMap,userName_23,password_23)
    }
    tencentCid2SidMap.getOrElse(cid,null)
  }


}
