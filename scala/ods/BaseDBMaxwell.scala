package ods

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: ods
 * ClassName: BaseDBMaxwell 
 *
 * @author 18729 created on date: 2020/12/3 9:51
 * @version 1.0
 * @since JDK 1.8
 */
object BaseDBMaxwell {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象 注意：Streaming程序至少不能设置为local，至少需要2个线程
    val conf: SparkConf = new SparkConf().setAppName("Spark01_W").setMaster("local[4]")
    //创建Spark Streaming上下文环境对象O
    val ssc = new StreamingContext(conf,Seconds(3))
    val gmallstartup = "ODS_DB_GMALL2020_M"
    val daugroup = "gmall_base_db_maxwell_group"
    val partitionToLong = util.OffsetManager.getOffset(gmallstartup, daugroup)
    var inputStream: InputDStream[ConsumerRecord[String, String]]=null
    if (partitionToLong!=null&&partitionToLong.size>0){
      inputStream = util.MyKafkaUtil.getKafkaStream(gmallstartup, ssc, partitionToLong, daugroup)
    }else{
      inputStream=util.MyKafkaUtil.getKafkaStream(gmallstartup,ssc)
    }

    var  offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val  inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges  //driver? executor?  //周期性的执行
      rdd
    }

    val value = inputGetOffsetDstream.map(rdd => {
      val str = rdd.value()
      val nObject = JSON.parseObject(str)
      nObject
    })

    value.foreachRDD(rdd=>{
      rdd.foreach(jsonObj=>{
        println(jsonObj.toString())
        val data = jsonObj.getString("data")
        val table = jsonObj.getString("table")
        val topic = "ODS_" + table.toUpperCase()
        util.MyKafkaSink.send(topic,data)
      })
    util.OffsetManager.saveOffset(gmallstartup, daugroup, offsetRanges)
    })
    //启动采集器
    ssc.start()
    //默认情况下，上下文对象不能关闭
    //ssc.stop()
    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
