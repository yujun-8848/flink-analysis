package com.yjun.networkflow

import java.sql.Timestamp
import java.{lang, util}
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class AppcheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetWorkFlow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val dataArrays: Array[String] = data.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val timeStamp: Long = dateFormat.parse(dataArrays(3).trim).getTime
        AppcheLogEvent(dataArrays(0).trim, dataArrays(1).trim, timeStamp, dataArrays(5).trim, dataArrays(6).trim)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AppcheLogEvent](Time.milliseconds(1)) {
      override def extractTimestamp(t: AppcheLogEvent): Long = t.eventTime
    }).keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new aggCount(), new windowFunciton())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
    dataStream.print()
    env.execute()

  }

  class aggCount() extends AggregateFunction[AppcheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: AppcheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class windowFunciton() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))

    }
  }

  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url", classOf[UrlViewCount]))

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

      urlState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      //从状态中拿到数据
      val allUrlViews = new ListBuffer[UrlViewCount]
      val iterator: util.Iterator[UrlViewCount] = urlState.get().iterator()
      while (iterator.hasNext) {
        val count: UrlViewCount = iterator.next()
        allUrlViews += count
      }
      urlState.clear()

      val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(topSize)
      //格式化结果输出
      val result = new StringBuilder
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (elem <- sortedUrlViews.indices) {
        val currentUrlViews: UrlViewCount = sortedUrlViews(elem)
        result.append("NO").append(elem + 1).append(": ")
          .append("URL= ").append(currentUrlViews.url)
          .append("访问量= ").append(currentUrlViews.count).append("\n")
      }
      result.append("==============================")
      Thread.sleep(1000)
      out.collect(result.toString())


    }
  }

}
