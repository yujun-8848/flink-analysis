import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LogAnalysis {

  case class Log(time: Long, ip: String, domains: String, count: Long)
  case class outLog(timeEnd: String, domains: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //step1:生产数据
    val data: DataStream[String] = env.addSource(new MockSource())
    val value: DataStream[Log] = data.map(x => {
      val strings: Array[String] = x.split("\t")
      val time: String = strings(3)
      var timeLong: Long = 0L
      try {
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        timeLong = format.parse(time).getTime
      } catch {
        case e: Exception => {
          print("error；time")
        }
      }
      Log(timeLong, strings(4), strings(5), strings(6).toInt)
    }).filter(_.time != 0)

    //step2:水印
    val datas: DataStream[outLog] = value.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Log](Time.seconds(1)) {
      override def extractTimestamp(element: Log): Long = {
        element.time
      }
    }).keyBy(_.domains)
      .timeWindow(Time.seconds(60))

      //step3:使用processWindowFunction处理数据
      .process(new WindowFunciotns())

  //  datas.print()

    env.execute("LogAnalysis")
  }


  class WindowFunciotns extends ProcessWindowFunction[Log, outLog, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Log], out: Collector[outLog]): Unit = {
      val end: Long = context.window.getEnd
      val str: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(end)
      val iterator: Iterator[Log] = elements.iterator
      var sum: Long = 0L
      while (iterator.hasNext) {
        val log: Log = iterator.next()
        sum += log.count
      }
      out.collect(outLog(str, key, sum))
    }
  }
}
