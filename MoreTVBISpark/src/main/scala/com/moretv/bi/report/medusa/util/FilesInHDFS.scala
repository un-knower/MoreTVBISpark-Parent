package com.moretv.bi.report.medusa.util

/**
 * Created by xiajun on 2016/5/19.
 * 从HDFS中获取文件
 */

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

object FilesInHDFS {
  def getFileFromHDFS(path:String):Array[FileStatus]={
    val dst = path
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val input_dir = new Path(dst)
    val hdfs_files = fs.listStatus(input_dir)
    hdfs_files
  }

  def fileIsExist(path:String,fileName:String)={
    var flag = false
    val files = getFileFromHDFS(path)
    files.foreach(file=>{
      if(file.getPath.getName==fileName){
        flag = true
      }
    })
    flag
  }


  //check if the directory contains _SUCCESS file
  def IsInputGenerateSuccess(path:String):Boolean={
    var flag = false
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    flag=fs.exists(new Path(path+File.separator+"_SUCCESS"))
    flag
  }

}
