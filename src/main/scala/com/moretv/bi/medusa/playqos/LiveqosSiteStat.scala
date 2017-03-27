package com.moretv.bi.medusa.playqos

import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 3/13/17.
  */
object LiveqosSiteStat extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

      }
      case None => {
        print("out")
      }
    }

  }
}
