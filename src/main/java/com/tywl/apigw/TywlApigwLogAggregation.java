package com.tywl.apigw;


import lombok.Data;

import java.util.Arrays;
@Data
public class TywlApigwLogAggregation {
    private String apiCode;
    private String apiName;
    private String appCode;
    private String appName;
    private Integer resCode;
    private String[] clusterTags;
    private String urlIn;
    private String catalog1Code;
    private String catalog1Name;
    private String remoteIp;
    private String catalog2Code;
    private String catalog2Name;
    private String[] serviceTags;
    private String userAccount;
    private String userId;
    private String userName;
    private String gwErrorCode;
    private String dealTime;
    private Long sumTimeGwDuration;
    private Long sumTimeThirdDuration;
    private Double avgTimeGwDuration;
    private Double avgTimeThirdDuration;
    private Long maxTimeGwDuration;
    private Long maxTimeThirdDuration;
    private Long minTimeGwDuration;
    private Long minTimeThirdDuration;
    private Long totalNum;
}
