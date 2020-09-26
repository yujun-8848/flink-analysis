import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogAnalysisJava {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.addSource(new MockSource());
        //获取数据源
        SingleOutputStreamOperator<Log> data = source.map(new MapFunction<String, Log>() {
            public Log map(String value) throws Exception {
                String[] split = value.split("\t");
                String time = split[3];
                long timeLong = 0L;
                SimpleDateFormat format = null;
                try {
                    format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date parse = format.parse(time);
                    timeLong = parse.getTime();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return new Log(timeLong, split[4], split[5], Long.parseLong(split[6]));
            }
        }).filter(new FilterFunction<Log>() {
            public boolean filter(Log value) throws Exception {
                return value.getTime() != 0L;
            }
        });
        //周期产生watermarks(时间驱动)
        SingleOutputStreamOperator<Log> watermarks = data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Log>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Log element) {
                return element.getTime();
            }
        });

        //数据驱动watermark
        data.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Log>() {
            @Nullable
            public Watermark checkAndGetNextWatermark(Log lastElement, long extractedTimestamp) {
                return lastElement.getDomains().equals("") ? new Watermark(extractedTimestamp):null;
            }

            public long extractTimestamp(Log element, long previousElementTimestamp) {
                return element.getTime();
            }
        });
        watermarks.keyBy(new KeySelector<Log, String>() {
            public String getKey(Log value) throws Exception {
                return value.getDomains();
            }
        }).timeWindow(Time.seconds(60))
                .process(new WindowFunciotns()).print();
        env.execute("LogAnalysis");

    }

    static class WindowFunciotns extends ProcessWindowFunction<Log, OutLog, String, TimeWindow> {
        @Override
        public void process(String value, Context context, Iterable<Log> elements, Collector<OutLog> out) throws Exception {
            //结束时间
            long end = context.window().getEnd();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String s = format.format(new Date(end));
            long count = 0;
            for (Log next : elements) {
                count += next.getCount();
            }
            out.collect(new OutLog(s, value, count));
        }
    }
}
