# 本项目在北京集群运行，用于从分析代码抽取维度逻辑，最后合并在事实表ETL中，落地一个新的维度丰富的parquet文件。

## 最终目的：
1.对事实表ETL【例如：playview日志】，填充空缺维度【例如subject code】，分析脚本写sql与维度表关联获得分析结果。
2.临时分析需求，使用HUE对应。

## 实施步骤：
1. 以具体分析代码作为切入口【例如，EachChannelSubjectPlayInfo，统计不同频道的专题播放量】
将对分析维度【例如，subjectCode】 的获取操作【例如，subjectCode不存在，需要subjectName去mysql里查询】
抽取出来【例如，ChannelEntrancePlayStatExample】。
2. 后续,注释中的代码，会统一合并为事实表的ETL操作【包含脏数据过滤，2.x and 3.x 合并】
/**最终此逻辑会合并进入事实表的ETL过程-start**/
/**最终此逻辑会合并进入事实表的ETL过程-end**/
这样做，方便分工合作和验证数据正确性。


## 维度逻辑解析进度
1. subjectCode【Done】
具体过程：subjectCode不存在，需要subjectName去维度表dim_medusa_subject里查询subjectCode补全
代码例子：
com.moretv.bi.report.medusa.channeAndPrograma.movie.EachChannelSubjectPlayInfoExample
2. 解析出各个频道的一级分类，二级分类，三级分类，四级分类入口维度【需要收集分布在资源调度平台的sql语句】
com.moretv.bi.report.medusa.newsRoomKPI.channelClassificationAnalyse【Doing】

## 分析脚本改造
### 工作内容:
1. 阅读原有分析脚本代码逻辑，将RDD写法逻辑改为使用DataFrame API，以操作表的方式做join，filter逻辑，
   关联维度表获取分析结果
2. 分析结果与线上结果验证比对

### 进度
1. 脚本作用：统计不同入口播放统计 【频道及栏目编排-频道概况-「电影」-不同入口播放统计】Done
com.moretv.bi.report.medusa.newsRoomKPI.ChannelEntrancePlayStatExample

2. 脚本作用：统计不同频道的专题播放量，用于展示在各个频道的专题播放趋势图以及内容评估的专题趋势图 Done
com.moretv.bi.report.medusa.channeAndPrograma.movie.EachChannelSubjectPlayInfoExample

3. 脚本作用：频道分类统计的播放次数播放人数统计 【Doing】
com.moretv.bi.report.medusa.newsRoomKPI.channelClassificationAnalyse



###--------------------------------开发测试--------------------------------
测试环境：bigdata-appsvr-130-2，bigdata-appsvr-130-3
测试目录：
cd /opt/bi/medusa/bin
sh submit.sh
1.启动例子
nohup sh submit.sh com.moretv.bi.report.medusa.newsRoomKPI.ChannelEntrancePlayStatExample \
  --startDate 20170321 \
  --deleteOld true     \
  >ChannelEntrancePlayStatExample.log 2>&1 &

2.查询mysql验证
北京线上数据库：
mysql -hbigdata-extsvr-db_bi1 -ubi -Dmedusa -pmlw321@moretv
北京测试数据库：
mysql -hbigdata-appsvr-130-1 -ubi -Dmedusa -pmlw321@moretv
###--------------------------------开发测试end--------------------------------

###--------------------------------本地jar包上传服务器--------------------------------
cp /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar ~/Documents/MoreTVBISpark-1.0.0-michael.jar
md5 /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar
###--------------------------------本地jar包上传服务器end--------------------------------
