package com.tywl.apigw;

import lombok.Data;

import java.util.Map;

@Data
public class NginxDataExtraction {
    /** 入访ip */
    private String remoteAddr;
    /** 网关服务ip */
    private String upstreamAddr;
    /** 网关url */
    private String request;
    /** 网关appKey */
    private String httpAppKey;
    /** 时间 */
    private String localTime;
    /** 时延 */
    private String requestTime;
    /** 状态码 */
    private String status;

    NginxDataExtraction(Map<String, String> fieldValuePairs) {
        this.setRemoteAddr(fieldValuePairs.get("$remote_addr"));
        this.setLocalTime(fieldValuePairs.get("$time_local"));
        this.setUpstreamAddr(fieldValuePairs.get("$upstream_addr"));
        this.setRequestTime(fieldValuePairs.get("$request_time"));
        this.setRequest(fieldValuePairs.get("$request"));
        this.setStatus(fieldValuePairs.get("$status"));
        this.setHttpAppKey(fieldValuePairs.get("$http_AppKey"));
    }
}
