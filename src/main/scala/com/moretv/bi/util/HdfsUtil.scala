package com.moretv.bi.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

/**
 * Created by Will on 2015/9/15.
 */
object HdfsUtil {

  def deleteHDFSFile(file:String){
    if(null!=file && !file.isEmpty){
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val path = new Path(file)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    }else{
      println("deleteHDFSFile failed")
    }
  }

  def getHDFSFileStream(file:String) = {
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val path = new Path(file)
    fs.open(path)
  }

  def copyFilesInDir(srcDir:String,distDir:String) :Boolean = {
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val isSuccess=FileUtil.copy(fs,new Path(srcDir),fs,new Path(distDir),false,false,conf)
    isSuccess
   }

  def getFileFromHDFS(path: String): Array[FileStatus] = {
    val dst = path
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val input_dir = new Path(dst)
    val hdfs_files = fs.listStatus(input_dir)
    hdfs_files
  }
}
