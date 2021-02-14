package com.gin.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object FlinkDemoTest {
  /**
    * 运行前先在node01上启动一个客户端通信端口
    *
    * nc -lk 8888
    *
    * nc命令安装：
    * yum install nc -y
    * yum install nmap -y
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //准备环境
    /**
      * createLocalEnvironment 创建一个本地执行的环境  local
      * createLocalEnvironmentWithWebUI 创建一个本地执行的环境  同时还开启Web UI的查看端口  8081
      * getExecutionEnvironment 根据你执行的环境创建上下文，比如local  cluster
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度为1，不启动过多线程进行执行(默认cpu核数，如有超线程也算）
    env.setParallelism(1)
    /**
      * DataStream：一组相同类型的元素 组成的数据流
      * 如果数据源是scoket  并行度只能是1
      */
    val initStream: DataStream[String] = env.socketTextStream("node01", 8888).setParallelism(1)
    //setParallelism 算子链 <-> 并行度
    val wordStream = initStream.flatMap(_.split(" ")).setParallelism(3)
    val pairStream =
      wordStream.map((_, 1)).setParallelism(3)
    val keyByStream = pairStream.keyBy(0)
    val restStream = keyByStream.sum(1).setParallelism(3)
    restStream.print()

    /**
      * 6> (msb,1)
      * 1> (,,1)
      * 3> (hello,1)
      * 3> (hello,2)
      * 6> (msb,2)
      * 默认就是有状态的计算
      * 6>  代表是哪一个线程处理的
      * 相同的数据一定是由某一个thread处理
      **/
    //启动Flink 任务
    env.execute("first flink job")
  }
}
