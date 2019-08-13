package com.fyj.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {
  def main(args: Array[String]): Unit = {

    var sc = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")
    var ssc = new StreamingContext(sc, Seconds(5))

    val blacks = List("zs","ls") //获取黑名单
    val blackRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))

    val lines = ssc.socketTextStream("192.168.243.20",6789)
    val clickLog = lines.map(x=>(x.split(",")(1),x)).transform(rdd=>{
      rdd.leftOuterJoin(blackRDD)
        .filter(x=>x._2._2.getOrElse(false)!=true)
        .map(x=>x._2._1)
    })

    clickLog.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
