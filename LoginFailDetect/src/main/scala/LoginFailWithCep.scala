import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    //1. 读取事件数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream: DataStream[String] = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = dataStream.map(data => {
      val dataArrays: Array[String] = data.split(",")
      LoginEvent(dataArrays(0).trim.toLong, dataArrays(1).trim, dataArrays(2).trim, dataArrays(3).trim.toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000
    }).keyBy(_.userId)

    //2. 定义模式匹配
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    //3. 在事件流上应用模式，得到一个pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    //4. 从pattern stream上应用select function,检出匹配事件序列
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()
    env.execute()

  }

}

class LoginFailMatch extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
    //从map中按照名称取出对应的事件
    val firstFail: LoginEvent = pattern.get("begin").iterator().next()
    val lastFail: LoginEvent = pattern.get("next").iterator().next()

    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "warning...")

  }
}
