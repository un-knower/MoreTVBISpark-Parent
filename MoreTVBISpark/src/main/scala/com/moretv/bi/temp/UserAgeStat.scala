package com.moretv.bi.temp

import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.runtime.AbstractFunction0

/**
  * Created by witnes on 10/14/16.
  */
object UserAgeStat extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(UserAgeStat, args)
  }

  override def execute(args: Array[String]): Unit = {
    val mySqlOps = new MySqlOps("ucenter")

    mySqlOps.getJdbcRDD(sc, sql, )
  }

}


//class DbConnection extends AbstractFunction0[con]