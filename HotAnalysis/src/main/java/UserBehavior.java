import lombok.AllArgsConstructor;
import lombok.Data;
@AllArgsConstructor
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private int categoryId;
    private String behavior;
    private Long timeStamp;


}
