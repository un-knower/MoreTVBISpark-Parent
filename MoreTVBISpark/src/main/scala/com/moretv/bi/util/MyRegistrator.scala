package com.moretv.bi.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by Will on 2015/8/10.
 */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    kryo.register(classOf[org.apache.spark.sql.DataFrame])
    kryo.register(classOf[String])

  }
}
