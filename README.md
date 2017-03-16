用于事实表的ETL操作，本项目在北京集群运行
测试环境：bigdata-appsvr-130-2
测试目录：
cd /opt/bi/medusa/bin
sh submit.sh 


cp /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar ~/Documents

cp /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/ ~/Documents


[spark@bigdata-appsvr-130-6 bin]$ mysql -h10.255.130.1 -ubi -Dmedusa -pmlw321@moretv

sh submit.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachChannelSubjectPlayInfoExample --startDate 20170310












后期准备修改：
use dw_fact_medusa;
CREATE EXTERNAL TABLE fact_medusa_play(
   dim_web_location_sk         BIGINT,
   dim_app_version_sk          BIGINT,
   dim_account_sk              BIGINT,
   dim_terminal_sk             STRING,
   dim_program_sk              STRING,
   dim_session_sk              STRING,
   dim_app_channel_sk          STRING,
   dim_date_key                STRING,
   dim_time_key                STRING,
   source_launcher_sk          STRING,
   source_page_sk              STRING,
   source_search_sk            STRING,
   source_list_sk              STRING,
   source_retrieval_sk         STRING,
   source_special_sk           STRING,
   source_recommend_sk         STRING,
   start_date                  STRING,
   start_time                  STRING,
   end_date                    STRING,
   end_time                    STRING,
   duration                    BIGINT,
   aid                         STRING,
   play_format                 STRING,
   program_duration            BIGINT,
   contain_ad                  STRING,
   auto_clarity                STRING,
   play_id                     STRING,
   episodeSid                  STRING,
   extraPath                   STRING,
   datetime                    STRING,
   event                       STRING,
   flag                        STRING,
   mark                        STRING,
   promotionChannel            STRING
  )
  PARTITIONED BY (day_p INT)
  STORED AS PARQUET
  LOCATION '/data_warehouse/dw_fact_medusa/fact_medusa_play';

  alter table fact_medusa_play add partition (day_p=20161201) location '/data_warehouse/dw_fact_medusa/fact_medusa_play/20161201';