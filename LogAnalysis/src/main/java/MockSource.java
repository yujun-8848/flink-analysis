import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class MockSource implements SourceFunction<String> {

    private boolean isRunning = true;

    public void run(SourceContext<String> ctx) throws Exception {
        StringBuilder sb = new StringBuilder();
        while (isRunning) {
            sb.append("imooc").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");
            Thread.sleep(1000);
            ctx.collect(sb.toString());
            sb.delete(0,sb.length());
        }
    }

    private long getTraffic() {
        return new Random().nextInt(10000);
    }

    private String getDomains() {
        String[] domains = new String[]
                {"v1.qwe.com", "v2.qwe.com", "v3.qwe.com", "v4.qwe.com"};
        return domains[new Random().nextInt(domains.length)];
    }

    private String getIps() {
        String[] ips = new String[]{"198.169.784", "198.169.783", "198.169.784"};
        return ips[new Random().nextInt(ips.length)];
    }

    private String getLevels() {
        String[] levels = new String[]{"M", "E"};
        return levels[new Random().nextInt(levels.length)];
    }


    public void cancel() {
        isRunning = false;
    }
}
