package com.tywl.apigw;


import lombok.Data;

import java.util.Arrays;
@Data
public class TywlApigwLog {
    private String apiCode;
    private String apiName;
    private String appCode;
    private String appName;
    private Integer resCode;
    private String[] tags;
    private String timeGwReq;
    private Integer timeGwDuration;
    private Integer timeThirdDuration;
    private String urlIn;
    private String regionCode;
    private String regionName;
    private String remoteIp;
    private String userName;
    private String userId;
    private String gwErrorCode;
    private String account;
    private String logType;
}
