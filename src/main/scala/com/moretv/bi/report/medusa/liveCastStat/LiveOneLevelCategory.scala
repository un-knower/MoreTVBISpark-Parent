package com.moretv.bi.report.medusa.liveCastStat

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by witnes on 2/15/17.
  */
object LiveOneLevelCategory {


  /**
    *
    * @param sourceType 'webcast'
    * @param sc
    * @param sqlContext
    * @return
    */
  def code2Name(sourceType: String, sc: SparkContext)(implicit sqlContext: SQLContext): DataFrame = {

    val q = sqlContext
    import q.implicits._
    import com.moretv.bi.report.medusa.liveCastStat.DimForLive._

    val db = DataIO.getMySqlOps(DataBases.MORETV_CMS_MYSQL)
    val url = db.prop.getProperty("url")
    val driver = db.prop.getProperty("driver")
    val user = db.prop.getProperty("user")
    val password = db.prop.getProperty("password")

    val (min, max) = db.queryMaxMinID("mtv_program_site", "id")
    db.destory()

    //只保留跑任务时生效的分类
    val sql =
      s"select code as liveMenuCode, name as liveMenuName from mtv_program_site " +
        s"where contentType = '$sourceType' AND STATUS = 1 AND ID >= ? AND ID <= ? "

    MySqlOps.getJdbcRDD(sc, sql, "mtv_program_site",
      r => (r.getString(1), r.getString(2)), driver, url, user, password, (min, max), 5)
      .toDF(groupFields4CodeName: _*)

  }

}
