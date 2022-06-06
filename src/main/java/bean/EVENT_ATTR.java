package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author qiang
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EVENT_ATTR {
  //EVENT_ID,ATTR_NAME,COLUMN_NAME
  private String EVENT_ID;
  private String ATTR_NAME;
  private String COLUMN_NAME;
}
