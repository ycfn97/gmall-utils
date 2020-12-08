package util

import java.util
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: util
 * ClassName: OffsetManager
 *
 * @author 18729 created on date: 2020/11/29 21:05
 * @version 1.0
 * @since JDK 1.8
 */
object OffsetManager {
  def saveOffset(gmallstartup: String, daugroup: String, offsetRanges: Array[OffsetRange]) = {
    val str = "offset:" + gmallstartup + ":" + daugroup
    val map: util.HashMap[String,String] = new util.HashMap()
    for (offset<-offsetRanges){
      val partition = offset.partition
      val offset1 = offset.untilOffset
      map.put(partition+"",offset1+"")
      println("写入分区："+partition +":"+offset.fromOffset+"-->"+offset1)
    }
    if (map!=null&&map.size()>0){
      val client = RedisUtil.getJedisClient
      client.hmset(str,map)
      client.close()
    }
  }

  def getOffset(gmallstartup: String, daugroup: String):Map[TopicPartition,Long]={
    val offsetKey = "offset:"+gmallstartup+":"+daugroup
    val client = RedisUtil.getJedisClient
    val str: util.Map[String, String] = client.hgetAll(offsetKey)
    client.close()
    import scala.collection.JavaConversions._
    val map: Map[TopicPartition, Long] = str.map { case (topicPartition, long) => {
      println("加载分区偏移量：" + topicPartition + ":" + long)
      (new TopicPartition(gmallstartup, topicPartition.toInt), long.toLong)
    }
    }.toMap
    map
  }
}
