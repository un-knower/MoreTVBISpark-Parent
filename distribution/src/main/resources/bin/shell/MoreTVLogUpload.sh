source ~/.bash_profile

DAY=`date -d 'today' +%Y%m%d`

HADOOP_HOME=/data/hadoopecosystem/hadoop

JAVA_HOME=/data/tools/jdk1.7.0_25

$HADOOP_HOME/bin/hadoop fs -put /data/moretv/bak/fulllog_bak/moretvlog.access.log-"$DAY"-portal2-1 /log/data/
