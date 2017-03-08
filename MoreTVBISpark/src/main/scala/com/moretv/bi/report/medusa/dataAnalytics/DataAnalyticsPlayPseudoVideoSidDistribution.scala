package com.moretv.bi.report.medusa.dataAnalytics

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by michael on 3/8/17.
  *
  * 统计相同videoSid，但videoName不同的记录中，videoSid对应不同videoName的数量分布
  *
  */
object DataAnalyticsPlayPseudoVideoSidDistribution extends BaseClass {

  private val tableName = "data_analytic_play_pseudo_videoSid_distribution"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          var sqlStr=""
          if (p.deleteOld) {
            val deleteSql = s"delete from $tableName where day = ?"
            println(s"deleteSql is $deleteSql")
            util.delete(deleteSql, insertDate)
          }
          val insertSql = s"insert into $tableName(day,videoSid,total_count) values(?,?,?)"
          println(s"insertSql is $insertSql")
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY).select("videoSid", "videoName").registerTempTable("log_data")
          val thresholdValue=100
          sqlStr=s"""
                   |select substring(videoSid,1,100) as videoSid,
                   |       count(distinct videoName) as total_count
                   |      from log_data
                   |      group by videoSid
                   |      having total_count>$thresholdValue
                 """.stripMargin
          println(sqlStr)
          sqlContext.sql(sqlStr).foreachPartition(partition => {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(rdd => {
              util1.insert(insertSql, insertDate, rdd.getString(0), rdd.getLong(1))
            })
          })
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}

/*
2-15 machine mysql ,create table
* use medusa;
CREATE TABLE `data_analytic_play_pseudo_videoSid_distribution` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `day` varchar(20) NOT NULL DEFAULT '',
  `videoSid` varchar(100) NOT NULL DEFAULT '' COMMENT 'videoSid',
  `total_count` bigint(40) NOT NULL DEFAULT '0' COMMENT 'videoName不同的个数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
* */

/*test evn,2-17
* 本地打包，bi_refactor分支,将MoreTVBISpark-1.0.0-SNAPSHOT.jar包上传到/script/bi/medusa/xiajun/BI_REFACTOR/MoreTVBISpark-1.0.0-SNAPSHOT-bin/lib/目录，
* 运行脚本模版参考/home/spark/cdh/bi_refactor.sh
*
* */


/*
#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "需要输入日期 like 20161201"
  exit 1
fi

one_day=$1
main_class="com.moretv.bi.report.medusa.dataAnalytics.DataAnalyticsPlayPseudoVideoSidDistribution"
echo ""
cd /script/bi/medusa/xiajun/BI_REFACTOR/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh submit.sh ${main_class} --startDate ${one_day} --deleteOld true

* */

