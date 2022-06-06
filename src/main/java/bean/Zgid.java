package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Zgid {
    //不要  字段  network   mccmnc  begin_time_id   begin_day_id utc_date
    private String ZG_ID;
    private String SESSION_ID;
    private String UUID;
    private String EVENT_ID;
    private String BEGIN_DATE;
    private String BEGIN_TIME_ID;
    private String DEVICE_ID;
    private String USER_ID;
    private String EVENT_NAME;
    private String PLATFORM;
    private String USERAGENT;
    private String WEBSITE;
    private String CURRENT_URL;
    private String REFERRER_URL;
    private String CHANNEL;
    private String APP_VERSION;
    private String IP;
    private String IP_STR;
    private String COUNTRY;
    private String AREA;
    private String CITY;
    private String OS;
    private String OV;
    private String BS;
    private String BV;
    private String UTM_SOURCE;
    private String UTM_MEDIUM;
    private String UTM_CAMPAIGN;
    private String UTM_CONTENT;
    private String UTM_TERM;
    private String DURATION;
    private String ATTR5;
    private String ORIG_JSON;


}
