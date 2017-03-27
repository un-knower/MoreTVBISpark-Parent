package com.moretv.bi.report.medusa

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.LogTypes
import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2017/1/5.
 */
object UserAreaInfoMap extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        sqlContext.udf.register("getCountry",IPLocationDataUtil.getCountry _)
        sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
        sqlContext.udf.register("getCity",IPLocationDataUtil.getCity _)

        val outDir = s"${LogTypes.USERAREAOUTDIR}${p.startDate}/userArea/"
        // 账号信息
        DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,p.startDate).
          select("user_id","ip").registerTempTable("log_data1")
        // 城市的维度表信息
        sqlContext.read.load(LogTypes.AREAINFO).registerTempTable("log_data2")
        sqlContext.sql(
          """
            |select a.user_id,getCity(a.ip) as city,getProvince(a.ip) as province,
            |getCountry(a.ip) as country,b.area,b.cityLevel
            |from log_data1 as a
            |left join
            |log_data2 as b
            |on getCity(ip)=b.city
          """.stripMargin).write.parquet(outDir)
      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

}
