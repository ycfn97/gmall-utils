package util

import java.io.InputStreamReader
import java.util.Properties

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: util
 * ClassName: PropertiesUtil 
 *
 * @author 18729 created on date: 2020/11/28 9:26
 * @version 1.0
 * @since JDK 1.8
 */
object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}

