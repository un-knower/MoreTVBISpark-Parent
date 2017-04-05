# 本项目在北京集群运行，用于从分析代码抽取维度逻辑，最后合并在事实表ETL中，落地一个新的维度丰富的parquet文件

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

a.频道分类入口统计
统计页面：【频道及栏目编排-频道分类统计-「电影,电视剧,咨询短片，综艺，动漫，纪实，戏曲」-频道分类统计】
原有方式为：
 1.写在资源调度平台里面【http://115.231.96.78:1337/login】
 2.原有分析代码，以sql方式编写,需要维护二级入口字段，
参考泰迪：http://172.16.17.100:8090/pages/viewpage.action?pageId=4424217

数据仓库方式改为：
解析维度：一级入口，二级入口
1.停止使用硬编码做二级入口过滤，改为正则匹配
2.使用站点树维度表dim_medusa_source_site过滤
3.在站点数里没有的二级分类入口，归入“其他分类”(原有展现已经有其他分类作为二级入口)
4.七个资源调度任务合成一个类com.moretv.bi.report.medusa.channelClassification.ChannelClassificationStatETL



b.统计不同频道的专题播放量
统计页面：【频道及栏目编排-节目与专题统计 「体育,动漫,少儿,电影,电视,纪实,资讯短片」】
原有方式为：
   1.需要查询mysql
    2.RDD写法
参考泰迪：http://172.16.17.100:8090/pages/viewpage.action?pageId=4424309

数据仓库方式改为：
解析维度：subject code
1.subjectCode补全移动到事实表ETL(关联维度表dim_medusa_subject补全)
2.Rdd改为DataFrame表方式分析
3.代码com.moretv.bi.report.medusa.subject.EachChannelSubjectPlayInfoETL



c.统计不同入口播放统计
统计页面：【频道及栏目编排-频道概况-「体育,动漫,音乐,少儿,电影,电视,综艺,纪实,戏曲,资讯短片」-不同入口播放统计 】
原有方式为：
    1.需要查询mysql
    2.RDD写法
参考泰迪：http://172.16.17.100:8090/pages/viewpage.action?pageId=4424092

数据仓库方式改为：
解析维度：首页入口
1.Rdd改为DataFrame表方式分析
2.代码com.moretv.bi.report.medusa.entrance.ChannelEntrancePlayStatETL


### 开发测试
测试环境：bigdata-appsvr-130-2，bigdata-appsvr-130-3
测试目录：
cd /opt/bi/medusa/bin
sh submit.sh
1.启动例子

nohup sh submit.sh com.moretv.bi.report.medusa.newsRoomKPI.ChannelEntrancePlayStatExample --startDate 20170321 --deleteOld true >ChannelEntrancePlayStatExample.log 2>&1 &

2.查询mysql验证
北京线上数据库：
mysql -hbigdata-extsvr-db_bi1 -ubi -Dmedusa -pmlw321@moretv
北京测试数据库：
mysql -hbigdata-appsvr-130-1 -ubi -Dmedusa -pmlw321@moretv

### 本地jar包上传服务器
1. 
cp /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar ~/Documents/MoreTVBISpark-1.0.0-michael.jar
2. 
md5 /Users/baozhiwang/Documents/nut/cloud/codes/MoreTVBISpark-Parent/target/MoreTVBISpark-1.0.0-release/lib/MoreTVBISpark-1.0.0.jar
