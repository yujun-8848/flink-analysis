import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 热门商品pv统计
 * 1.etl处理
 * 2.以itemId为key进行聚合，对每个窗口中的itemId进行预聚合,并与窗口进行绑定
 * 3.对windowEnd进行keyby,使用process对窗口中的值进行topN排序
 * 4.打印输出
 */
public class HotItems {

    public static void main(String[] args) throws Exception {
        String path = HotItems.class.getClass().getResource("/UserBehavior.csv").getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile(path);
        //整理好数据源和watermark
        SingleOutputStreamOperator<UserBehavior> pv = source.map(new MapFunction<String, UserBehavior>() {
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(UserBehavior element) {
                return element.getTimeStamp() * 1000L;
            }
        });
        //按itemId进行排序，并对每个窗口进行聚合操作，包装成ItemViewCount进行输出
        SingleOutputStreamOperator<String> result = pv.keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getItemId();
            }
        })
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new AggreCount(), new Window())
                .keyBy(new KeySelector<ItemViewCount, Long>() {
                    @Override
                    public Long getKey(ItemViewCount value) throws Exception {
                        return value.getWindowEnd();
                    }
                })
                .process(new processResult(5));
        result.print();
        env.execute("HotItems");
    }

    //对每个窗口，按照itemId进行预聚合，统计count值
    static class AggreCount implements AggregateFunction<UserBehavior, Long, Long> {
        public Long createAccumulator() {
            return 0L;
        }

        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        public Long getResult(Long accumulator) {
            return accumulator;
        }

        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    //将预聚合的值与window进行包装
    static class Window implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
        public void apply(Long value, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long windowEnd = window.getEnd();
            Long next = input.iterator().next();
            out.collect(new ItemViewCount(value, windowEnd, next));
        }
    }

    //设置定时器，并存储状态进行topN排序
    static class processResult extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private int top;
        private ListState<ItemViewCount> listState = null;

        public processResult(int top) {
            this.top = top;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("list-state", ItemViewCount.class));
        }

        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            //设置定时器，延迟1ms，当watermark到达时所有数据已存到list中
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> res = new ArrayList<>();
            Iterator<ItemViewCount> iterator = listState.get().iterator();
            while (iterator.hasNext()) {
                res.add(iterator.next());
            }
            //按照count的逆序进行排序
            res.sort((o1, o2) -> (int) (o2.getCount() - o1.getCount()));
            //清空状态
            listState.clear();
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间： ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < top; i++) {
                ItemViewCount itemViewCount = res.get(i);
                sb.append("NO").append(i + 1).append(":\t")
                        .append("商品ID = ").append(itemViewCount.getItemId()).append("\t")
                        .append("热门度 = ").append(itemViewCount.getCount()).append("\n");
            }
            sb.append("=======================\n");
            Thread.sleep(1000);
            out.collect(sb.toString());
        }
    }
}
