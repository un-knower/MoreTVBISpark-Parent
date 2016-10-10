package com.moretv.bi.apart

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by mycomputer on 1/22/2016.
  */
class MyRegistrator extends KryoRegistrator{
  override def registerClasses(kryo:Kryo): Unit ={
    kryo.register(classOf[java.lang.String])
  }
}
