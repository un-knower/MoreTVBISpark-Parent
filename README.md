
测试环境:
运行机器：2-17 machine
运行目录：/script/bi/medusa/xiajun/BI_REFACTOR/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
运行脚本
上传本地编译脚本:

cd /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/distribution/target/MoreTVBISpark-1.0.0-SNAPSHOT-bin/lib/
scp -rC MoreTVBISpark-1.0.0-SNAPSHOT.jar spark@bigdata-computing-02-017:/script/bi/medusa/xiajun/BI_REFACTOR/MoreTVBISpark-1.0.0-SNAPSHOT-bin/lib/




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