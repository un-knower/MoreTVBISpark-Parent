#!/bin/bash

###for restart process
###2016/10/08

function Usage(){
cat <<HELP
        usage: ./`basename $0`   start|stop|restart|status 
HELP
}

###变量定义
action=$1
version=$2
scriptName=`basename $0`
scriptDir=$(cd `dirname $0`;pwd)
projectName=${scriptDir##*/}
server_pid="${projectName}.pid"
process_flag="java"

###定义脚本日志函数
function write_exec_log(){
    ###函数参数
    log_level=$1
    log_mesg=$2

    ###日志目录变量
    log_dir="/tmp"
    log_file="all_do.log"
    log_path="$log_dir/$log_file"

    ###建立日志目录
    if [ ! -d "$log_dir" ];then
        mkdir -p $log_dir
    fi

    ###创建日志文件
    if [ ! -f "$log_path" ];then
        touch $log_path
    fi

    ###日志文件权限
    chmod 666 "$log_path"

    echo "`date +%Y-%m-%d\ %T`  $log_level  $log_mesg"|tee -a $log_path
}

###定义脚本锁函数
function lock_script(){
    pid_file="/tmp/${scriptName}.pid"
    lockcount=0
    script_id=`echo $$`
    while [ $lockcount -le 3 ];do
        if [ -f "$pid_file" ];then
            process_id=`head -n 1 $pid_file`
            same_arguments=`ps $process_id|grep -w $scriptName|wc -l`
            if [ "$same_arguments" -ge 1 ];then
                write_exec_log "error" "The script is running......"
                sleep 30
                let lockcount++
            else
                break
            fi
        else
            break
        fi
    done

    ###进程pid写入文件
    echo $$ >$pid_file

    ###pid文件赋权
    chmod 666 "$pid_file"
}

###判断执行用户及检验版本
function check_user(){
    ###判断执行用户
    if [ x`whoami` != xmoretv ];then
        write_exec_log "error" "user is must moretv"
        exit 2
    fi

    ###判断版本
    read_ver=`head -n 1 $scriptDir/version.txt`
    if [ x$version != x ];then
        if [ x"$version" != x"$read_ver" ];then
            write_exec_log "error" "$version not eq $read_ver"
            exit 2
        fi
    fi
}

###查看进程状态
function status_process(){
    ###进程类型变量
    process_have=`lsof $scriptDir 2>/dev/null|awk '{if($1=="'$process_flag'") print $0}'`

    ###判断进程是否存在
    if [ -n "$process_have" ];then
        return 0
    else
        return 1
    fi
}

###起进程
function start_process(){
    ###判断进程是否已经存在
    status_process
    process_judge=$?
    if [ $process_judge -eq 0 ];then
         write_exec_log "info" "The $projectName is already running"
         exit 2
    fi 

    ###查找jar包
    lib_dir="$scriptDir/lib"    
    jar_name=`ls $lib_dir|grep $projectName`

    ###判断有无jar包
    if [ -z "$jar_name" ];then
        write_exec_log "error" "$lib_dir not have $projectName jar"
    fi

    ###起进程
    cd $scriptDir
    nohup java -Dloader.path=lib/,conf/ -Xms2048m -Xmx2048m -XX:+HeapDumpOnOutOfMemoryError -jar lib/$jar_name >/dev/null 2>&1 &
    echo $! > $scriptDir/$server_pid

    ###判断进程是否起来
    sleep 5
    if [ $? -ne 0 ];then
        write_exec_log "error" "$projectName start error"
        exit 2 
    fi
}

###停进程
function stop_process(){
    ###根据文件和进程方式
    file_pid=`cat $scriptDir/$server_pid`
    proc_pid=`lsof $scriptDir 2>/dev/null|awk '{if($1=="'$process_flag'") print $2}'`
    kill -2 $file_pid $proc_pid 2>/dev/null

    ###循环多次停进程
    sleep 5
    stopcount=0
    while [ $stopcount -lt 3 ];do
        status_process
        process_judge=$?
        if [ $process_judge -eq 0 ];then
            sleep 5
            kill -9 $file_pid $proc_pid 2>/dev/null
            let stopcount++
        else
            break
        fi
    done

    ###判断进程停止
    if [ $stopcount -ge 3 ];then
        write_exec_log "error" "$projectName stop error"
        exit 2
    fi
}

lock_script
check_user

case "$action" in
    "stop")
        echo "no supported!"
    ;;
    "start")
        echo "no supported!"
    ;;
    "restart")
        echo "no supported!"
    ;;
    "status")
        echo "no supported!"
    ;;
    *)
       Usage
    ;;
esac
echo "success"
