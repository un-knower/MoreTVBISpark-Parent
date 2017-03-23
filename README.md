用于事实表的ETL操作，本项目在北京集群运行
测试环境：bigdata-appsvr-130-2
测试目录：
cd /opt/bi/medusa/bin
sh submit.sh 


本地jar包上传服务器:
cp /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar ~/Documents
md5 /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar 

统计不同入口播放统计：
nohup sh submit.sh com.moretv.bi.report.medusa.newsRoomKPI.ChannelEntrancePlayStatExample \
  --startDate 20170321 \
  --deleteOld true     \
  >ChannelEntrancePlayStatExample.log 2>&1 &

sh submit.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendUpgradeInfo --startDate 20170321 --deleteOld true



[spark@bigdata-appsvr-130-6 bin]$ 
mysql -h10.255.130.1 -ubi -Dmedusa -pmlw321@moretv
mysql -hbigdata-extsvr-db_bi1 -ubi -Dmedusa -pmlw321@moretv
mysql -hbigdata-appsvr-130-1 -ubi -Dmedusa -pmlw321@moretv

sh submit.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendUpgradeInfo --startDate 20170321 --deleteOld true

 create table  contenttype_play_src_stat_test like  contenttype_play_src_stat; 


select * from medusa_channel_subject_play_info where day='2017-03-18' order by channel_name;

select * from medusa_channel_subject_play_info_test where day='2017-03-18' order by channel_name;

  select sum(play_num) from medusa_channel_subject_play_info where day='2017-03-15' ;
 
 select sum(play_user),sum(play_num) from medusa_channel_subject_play_info where day='2017-03-14';
 select sum(play_user) from medusa_channel_subject_play_info_test where day='2017-03-15';
 
nohup sh submit.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachChannelSubjectPlayInfoExampleV2 --startDate 20170319 --deleteOld true \
>a.log 2>&1 &


select * from contenttype_play_src_stat where day='2017-03-'


SELECT * FROM `medusa_channel_subject_play_info` WHERE day='2017-03-15' order BY channel_name






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