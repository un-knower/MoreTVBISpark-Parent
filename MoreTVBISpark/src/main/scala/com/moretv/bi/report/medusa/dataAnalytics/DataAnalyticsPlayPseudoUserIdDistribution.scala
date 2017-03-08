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
  * 统计userId是同一个数值，但是apkVersion,productModel,promotionChannel的组合不同的分布
  *
  */
object DataAnalyticsPlayPseudoUserIdDistribution extends BaseClass {

  private val tableName = "data_analytic_play_pseudo_userid_distribution"

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
          val insertSql = s"insert into $tableName(day,apkVersion_productModel_promotionChannel,dimension_count,total_count) values(?,?,?,?)"
          println(s"insertSql is $insertSql")
          //临时没有该parquet文件,容错
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW).select("userId", "apkVersion", "productModel",
            "promotionChannel").unionAll(
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY).select("userId", "apkVersion", "productModel",
              "promotionChannel")).registerTempTable("log_data")

          //for test
          val result0=sqlContext.sql("select count(1) from log_data")
          println("result0.collect().size:"+result0.collect().head)

          /*统计同一个userId，但是apkVersion,productModel,promotionChannel不同的userId*/
          sqlContext.sql(
            """
              |select distinct userId
              |from (select userId,
              |             count(distinct concat(apkVersion,productModel,promotionChannel)) as total_count
              |      from log_data
              |      group by userId
              |      having total_count>1
              |      ) as tmp_table
            """.stripMargin).registerTempTable("user_table")

          //for test
          val result1=sqlContext.sql("select count(1) from user_table")
          println("result1.collect().size:"+result1.collect().head.getLong(0))

          /*统计同一个userId，但是apkVersion,productModel,promotionChannel不同，
            apkVersion,productModel,promotionChannel的分布情况*/
          sqlStr="""
            |select concat(apkVersion,'|',productModel,'|',promotionChannel) as apkVersion_productModel_promotionChannel
            |       count(userId) as dimension_count
            |from log_data a join user_table b on a.userId=b.userId
            |group by concat(apkVersion,'|',productModel,'|',promotionChannel)
          """.stripMargin
          sqlContext.sql(sqlStr).foreachPartition(partition => {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(rdd => {
              util1.insert(insertSql, insertDate, rdd.getString(0), rdd.getLong(1),0)
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
CREATE TABLE `data_analytic_play_pseudo_userid_distribution` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `day` varchar(20) NOT NULL DEFAULT '',
  `apkVersion_productModel_promotionChannel` varchar(20) NOT NULL DEFAULT '' COMMENT 'apkVersion|productModel|promotionChannel',
  `dimension_count` bigint(40) NOT NULL DEFAULT '0' COMMENT 'apkVersion_productModel_promotionChannel组合维度的人数之和',
  `total_user` bigint(40) NOT NULL DEFAULT '0' COMMENT '总人数',
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
main_class="com.moretv.bi.report.medusa.dataAnalytics.DataAnalyticsPlayPseudoUserIdDistribution"
echo ""
cd /script/bi/medusa/xiajun/BI_REFACTOR/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh submit.sh ${main_class} --startDate ${one_day} --deleteOld true

* */

