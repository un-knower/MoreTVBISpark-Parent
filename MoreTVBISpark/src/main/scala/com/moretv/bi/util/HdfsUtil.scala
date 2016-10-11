package com.moretv.bi.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by Will on 2015/9/15.
 */
object HdfsUtil {

  def deleteHDFSFile(file:String){
    val conf = new Configuration()
    val fs= FileSystem.get(conf)

    val path = new Path(file)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }

  def getHDFSFileStream(file:String) = {
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val path = new Path(file)
    fs.open(path)
  }

}
