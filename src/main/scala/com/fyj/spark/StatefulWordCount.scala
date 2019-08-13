package com.fyj.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用SparkStreaming完成有状态统计
  */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {

    //基本配置代码
    var sc = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")
    var ssc = new StreamingContext(sc, Seconds(5))

    //如果使用了带状态的stateful的算子，就一定要设置checkpoint，既然我们要做的是新值追加老值的操作，所以需要将老值存放起来才能追加
    //因为这里是测试环境， 如果是生产环境就要把目录设置到HDFS文件夹
    ssc.checkpoint(".")

    //业务代码,还是使用socket的方式
    val lines = ssc.socketTextStream("192.168.243.20",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()


    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues 当前的
    * @param preValues 老的
    * @return
    */
  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int]= {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }
}
