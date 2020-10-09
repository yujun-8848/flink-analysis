import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;

}
