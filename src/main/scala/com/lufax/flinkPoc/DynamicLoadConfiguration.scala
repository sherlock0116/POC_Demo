package com.lufax.flinkPoc

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.util.Collector
import org.apache.logging.log4j.scala.Logger

import scala.util.Random

/**
  * @Author:sherlock
  * @Description: // TODO
  * @Date: Created in 0:39 2019/8/8
  * @Modified By:
  */
object DynamicLoadConfiguration {

    def main(args: Array[String]): Unit = {

        val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val CONFIG_KEYWORDS: MapStateDescriptor[String, String] = new MapStateDescriptor[String, String](
            "config-keywords",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
        )

        // 广播流
        val broadcastStream = streamEnv
                .addSource(new MyStreamFunction("control"))
                .setParallelism(1)
                .broadcast(CONFIG_KEYWORDS)
        // 源数据流
        val sourceStream = streamEnv
                .addSource(new MyStreamFunction("source"))
                .setParallelism(1)

        sourceStream
                .connect(broadcastStream)
                .process(new MyBroadcastProcessFunction)
                .print()

        streamEnv.execute("intercept dynamic keyword")
    }
}

class MyStreamFunction(param: String) extends RichParallelSourceFunction[String] {


    private [this] lazy val LOGGER = Logger(this.getClass)

    @volatile private var isRunning = true
    private val keywords = Array("java", "php", "python","scala")
    private val streamSet = Array(
        "java是世界上最好语言",
        "php是世界上最好语言",
        "scala是世界上最好语言",
        "python是世界上最好语言",
        "go是世界上最好语言"
    )

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
    }

    /*
        根据参数判断: 控制流 or 源数据流
            控制流:模拟10秒随机更新一次拦截的关键词
            数据流:模拟每秒发送一条数据
  */
    override def run(src: SourceContext[String]): Unit = {
        val keyWordLength = keywords.length
        val streamSetSize = streamSet.length
        while (isRunning) {
            var seed: Int = -1
            param match {
                case "control" =>
                    seed = Random.nextInt(keyWordLength)
                    var keyWord = keywords(seed)
                    LOGGER.info(s"=======[关键词]: $keyWord")
                    src.collect(keyWord)
                    Thread.sleep(10 * 1000)

                case "source" =>
                    seed = Random.nextInt(streamSetSize)
                    var sourceStream = streamSet(seed)
                    LOGGER.info(s"*******[源数据]: $sourceStream")
                    src.collect(sourceStream)
                    Thread.sleep(1 * 1000)

                case _ =>   LOGGER.error("please check you parameter, it is either 'control' or 'source'!")
            }
        }
    }

    override def cancel(): Unit = {
        isRunning = false
    }

}

class MyBroadcastProcessFunction extends BroadcastProcessFunction[String, String, String] {

    lazy private [this] val LOGGER = Logger(this.getClass)
    private [this] var keyword: String = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        keyword = "java"
        println(s"初始化,模拟连接读取数据库拦截词为: $keyword")
    }

    override def processElement(
                                       in1: String,
                                       readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                       collector: Collector[String]): Unit = {
        if (in1.contains(keyword))
            collector.collect(s"拦截消息: $in1, 原因: 该消息中包含关键词: $keyword")
    }

    override def processBroadcastElement(
                                                in2: String,
                                                context: BroadcastProcessFunction[String, String, String]#Context,
                                                collector: Collector[String]): Unit = {
        keyword = in2
        collector.collect(s"关键词更新成功, 当前关键词为: $in2")
    }
}



