package com.moretv.bi.util
import java.nio.file.Path
import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by Will on 2015/7/17.
  */
object FileUtils {

  @deprecated
  def withCsvWriterOld(path: String, charset: String = "GBK")(op: PrintWriter => Unit) = {
    val out = new PrintWriter(path, charset)
    op(out)
    out.close()
  }

  def withCsvWriter(filename: String, charset: String = "GBK")(op: PrintWriter => Unit) = {
    val out = new PrintWriter("/home/moretv/liankai.tmp/share_dir/" + filename, charset)
    op(out)
    out.close()
  }



}
