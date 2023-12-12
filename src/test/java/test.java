import com.alibaba.fastjson.JSON;
import com.tywl.apigw.TywlApigwLog;
import com.tywl.apigw.TywlApigwLogAggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class test {
    public static void main(String[] args) {
         String str ="{\"log\":{\"file\":{\"path\":\"/home/apigw/logs/aifgw-backend-1.0/gateway/apigw-north-6db9ff54d9-l6tf7-gateway-back.log\"},\"offset\":913723474},\"method\":\"post\",\"timeGwRes\":\"2023-10-14 22:14:59.514\",\"apiName\":\"历史位置查询API_V2\",\"filepath\":\"com.asiainfo.aifgw.http.dataaccess.AccessDataProtocol:301\",\"account\":\"29000118112\",\"thread\":\"AiGw-ClientToAiGwWorker-18\",\"urlIn\":\"/openapi/v1/map/getHisLocation\",\"urlOut\":\"/openapi/v1/map/getHisLocation\",\"host\":{\"os\":{\"family\":\"debian\",\"codename\":\"focal\",\"version\":\"20.04.4 LTS (Focal Fossa)\",\"kernel\":\"4.4.246-1.el7.elrepo.x86_64\",\"type\":\"linux\",\"platform\":\"ubuntu\",\"name\":\"Ubuntu\"},\"ip\":[\"172.26.1.47\"],\"architecture\":\"x86_64\",\"containerized\":true,\"name\":\"ds-filebeat-north-c6lrt\",\"mac\":[\"9e:3c:97:0a:89:da\"],\"hostname\":\"ds-filebeat-north-c6lrt\"},\"apiCode\":\"F5GCMP_getHisLocation_V2\",\"@version\":\"1\",\"resCode\":200,\"userId\":\"20211214000002\",\"ecs\":{\"version\":\"1.12.0\"},\"class\":\"c.a.a.http.dataaccess.AccessDataProtocol\",\"msg\":\"93FC12B746F6E83AF1DF11E901FF7B51D8C6C009C571268735442FB43901DEFA58A9C4C50786A1E9ED40654522205C12ECFA1C477918581547DA88BF86D9CA73841F8DE5EC24F6B31F0F09C629095511F59DB2EF0A74FDAF52784C1FE6AD99ECF3C50C2D9BDFEB5F5143A04E182BD4756405529A2CBF3A5EED68265207DFD81C5143A04E182BD47560C9C03988029179A511BCC400B8E73FB87B85306080C5EDDE3138F92615433C22BFB13C1DB65FB05AD80128AB3EE4EC\",\"timeThirdDuration\":274,\"timeThirdReq\":\"2023-10-14 22:14:59.239\",\"tags\":[\"North-gw-service\"],\"date\":\"2023-10-14 22:14:59.514\",\"remoteIp\":\"47.119.174.103\",\"apiVersionName\":\"V1\",\"format\":\"json\",\"@timestamp\":\"2023-10-14T14:14:59.514Z\",\"appCode\":\"app_code_202305111127087560846\",\"timeGwReq\":\"2023-10-14 22:14:59.238\",\"timeThirdRes\":\"2023-10-14 22:14:59.513\",\"javaOpt\":{\"pod.clusterName\":\"north_apigw\",\"pod.direction\":\"\",\"pod.instanceName\":\"apigw-north-6db9ff54d9-l6tf7\"},\"protocol\":\"HTTP/1.1\",\"agent\":{\"version\":\"7.17.4\",\"id\":\"2a6c8bc7-afa2-47a0-a8e5-359892bc4923\",\"type\":\"filebeat\",\"name\":\"ds-filebeat-north-c6lrt\",\"ephemeral_id\":\"caa41fe7-3e38-453a-aa46-e1fd02622e32\",\"hostname\":\"ds-filebeat-north-c6lrt\"},\"cloud\":{\"instance\":{\"id\":\"0fb7e343-d2cb-4124-ae1e-724fc4e4c705\"},\"availability_zone\":\"cn-jssz1c\",\"provider\":\"huawei\",\"region\":\"\",\"service\":{\"name\":\"ECS\"}},\"level\":\"INFO\",\"appName\":\"鲁邦通二级账户api权限\",\"logType\":\"billing\",\"fields\":{\"level\":\"apigw\",\"review\":1},\"input\":{\"type\":\"log\"},\"apiVersion\":\"1.0.0\",\"timeGwDuration\":276,\"gatewayTraceId\":\"6264316b-3fbb-415c-ac4b-9131a7e8a37c\"}";

        TywlApigwLog tywlApigwLog = new TywlApigwLog();
        tywlApigwLog =  JSON.parseObject(str,TywlApigwLog.class);
        System.out.println(JSON.toJSONString(tywlApigwLog));
        List<TywlApigwLogAggregation> groupMapList  = new ArrayList<>();
        List<TywlApigwLog> tywlApigwLogList = new ArrayList<>();
        if (tywlApigwLog!=null && tywlApigwLog.getApiCode()!=null) {
            tywlApigwLogList.add(tywlApigwLog);
        }
        Map<String, List<TywlApigwLog>> mapListGroupByName = tywlApigwLogList.stream().collect(Collectors.groupingBy(p ->
                p.getAccount()+"#"+p.getGwErrorCode()+"#"+ Arrays.toString(p.getTags()) +"#"+p.getApiCode()+"#"+p.getApiName()+"#"+
                        p.getAppCode()+"#"+p.getAppName()+"#"+p.getResCode()+"#"+p.getRegionCode()+"#"+p.getRegionName()
                        +"#"+p.getRemoteIp()+"#"+p.getUserName()+"#"+p.getUserId()
                        +"#"+p.getTimeGwReq().substring(0,13)+":00:00"
        ));
        System.out.println(JSON.toJSONString(mapListGroupByName));
        mapListGroupByName.forEach((groupName, mapByNameList) -> {
            System.out.println(groupName);
        });


    }
}
