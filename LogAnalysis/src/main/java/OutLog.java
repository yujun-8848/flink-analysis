import lombok.AllArgsConstructor;
import lombok.Data;
@AllArgsConstructor
@Data
public class OutLog {

   // timeEnd: String, domains: String, count: Long
    private String timeEnd;
    private String domains;
    private long count;


}
