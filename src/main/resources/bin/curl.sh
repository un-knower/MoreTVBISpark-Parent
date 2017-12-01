#!/bin/sh
weburl=http://azkaban.whaley.cn
if [ $# -lt 2 ]; then
    echo " usage : `basename  $0`  projectname  flowname"
    exit 1
fi
project=$1
flow=$2
username=$3
password=$4
##获取sessionid
echo "================getssionid====================="
echo 'getssionid : curl -k -X POST --data "action=login&username=azkaban&password=azkaban" ' $weburl 
sessionid=`curl -k -X POST --data "action=login&username=$username&password=$password" $weburl | jq '."session.id"' `
sessionid=${sessionid//\"/""}
echo "================sessionid is $sessionid =============="
if [[ ${#sessionid} < 30 ]]; then
  echo "get sessionid fail , please check up curl command"
  exit 2
fi

echo "=================拼接执行一个flow 的参数======================="
Params=($@)
args=""
i=4
while [ $i -lt $# ]
do
  key=${Params[$i]}
  i=$(($i + 1))
  value=${Params[$i]}
  args="${args} --data 'flowOverride[$key]=$value'"
  i=$(($i + 1))
done
echo "=============拼接执行参数为 : ${args}==============================="

echo "=================执行一个flow 并且获取其execid======================="

echo "execution flow : curl -k --get --data 'session.id='$sessionid  --data 'ajax=executeFlow' --data 'project='$project  --data 'flow='$flow  --data \"failureAction=finishPossible\"  --data \"concurrentOption=ignore\" ${args}  $weburl/executor"
exeCommand="curl -k --get --data \"session.id=$sessionid\"  --data \"ajax=executeFlow\" --data \"project=$project\"  --data \"flow=$flow\"    --data \"failureAction=finishPossible\" --data \"concurrentOption=ignore\"  ${args}  $weburl/executor "
executionReuslt=` eval $exeCommand `
#executionid=`curl -k --get --data "session.id=$sessionid"  --data "ajax=executeFlow" --data "project=$project"  --data "flow=$flow"  --data "flowOverride[$3]=xxxxxxxxxxxxxxxxxxxxxxxxxx"  $weburl/executor | jq '.execid' `
executionid=`echo $executionReuslt | jq '.execid'`

if ([ $executionid == "" ] || [ $executionid == null ]) ;then
 echo "======================flow 调用失败 =========  ============================="
 echo "execute a flow is  fail , please check up curl command"
 exit 3
fi

echo "===============executionid is $executionid============================"

echo "===============通过execid获取flow执行状态============================="
echo 'get flow status : curl -k --data "execid='$executionid'&lastUpdateTime=-1&session.id='$sessionid'"'  $weburl/executor?ajax=fetchexecflowupdate
 
flowexecutionstatus=`curl -k --data "execid=$executionid&lastUpdateTime=-1&session.id=$sessionid"  $weburl/executor?ajax=fetchexecflowupdate | jq .'status' `
sleep 5s
while ([ $flowexecutionstatus != '"FAILED"' ] && [ $flowexecutionstatus != '"SUCCEEDED"' ]) 
do
  echo "sleep 30s ..."
  sleep 10s
  flowexecutionstatus=`curl -k --data "execid=$executionid&lastUpdateTime=-1&session.id=$sessionid"  $weburl/executor?ajax=fetchexecflowupdate | jq .'status' `
  echo $flowexecutionstatus
done

if [[ $flowexecutionstatus == '"FAILED"' ]];then
  echo "flow execution  status is FAILED ... "
  exit 4
fi

echo "success ...."
