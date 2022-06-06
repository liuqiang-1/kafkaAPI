package com.obei.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Zgid {
    //不要  字段  network   mccmnc  begin_time_id   begin_day_id utc_date
    private int ZG_ID;
    private String SESSION_ID;
    private String UUID;
    private String EVENT_ID;
    private String BEGIN_DATE;
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
    private String CUS1 ;
    private String CUS2 ;
    private String CUS3 ;
    private String CUS4 ;
    private String CUS5 ;
    private String CUS6 ;
    private String CUS7 ;
    private String CUS8 ;
    private String CUS9 ;
    private String CUS10 ;
    private String CUS11 ;
    private String CUS12 ;
    private String CUS13 ;
    private String CUS14 ;
    private String CUS15 ;
    //把pr 里面的json也封装进来
    private String JSON_PR;



}
