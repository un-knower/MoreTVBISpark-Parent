package com.moretv.bi.temp.user

import com.moretv.bi.util.ParamsParseUtil
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 11/8/16.
  */
object HistoryJob extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(HistoryJob, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil parse args match {
      case Some(p) => {

        import org.apache.spark.sql.{SQLContext, Row, DataFrame}

        import org.apache.spark.sql.types._

        val sqlContext = SQLContext.getOrCreate(sc)

        val csvFile = p.srcPath

        val struct =
          StructType(
            StructField("videoSid", StringType, false) ::
            StructField("videoName", StringType, false ) ::
            StructField("startTime", StringType, false) ::
            StructField("endTime", StringType, false) :: Nil
          )



      }
      case None => {}
    }
  }
}
