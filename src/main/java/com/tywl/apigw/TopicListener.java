package com.tywl.apigw;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
@Slf4j
public class TopicListener {
    private static final String[] fieldNames;
    static {
        fieldNames = "$remote_addr||$remote_user||$time_local||$http_x_forwarded_for||$http_true_client_ip||$upstream_addr||$upstream_response_time||$request_time||$hostname||$host||$http_host||$request||$status||$body_bytes_sent||$http_referer||$http_user_agent||$http_AppKey"
                    .split("\\|\\|");
    }
//    @KafkaListener(topics = {"south-nginx"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void southNginxExtractionConsumer(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        try (FileWriter fileWriter = new FileWriter("south-nginx-message.txt", true)) {
            for (ConsumerRecord<?, ?> record: consumerRecords) {
                String message = JSON.parseObject(record.value().toString()).getString("message");
                fileWriter.write(extractKeyFieldValues(message) + "\n");
            }
            log.info("消息写入文件成功！获取数据{}条，耗时{}ms", consumerRecords.size(),System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("消息写入文件失败。");
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"north-app"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void extractionConsumer(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        try (FileWriter fileWriter = new FileWriter("north-app-message.txt", true)) {
            for (ConsumerRecord<?, ?> record: consumerRecords) {
                JSONObject message = JSON.parseObject(record.value().toString());
                String type = message.getString("logType");
                if (!type.equals("billing")) {
                    continue;
                }
                String keyValues = message.getString("apiCode") + "||" +
                        message.getString("apiName") + "||" +
                        message.getString("appCode") + "||" +
                        message.getString("appName") + "||" +
                        message.getString("date") + "||" +
                        message.getString("timeGwReq") + "||" +
                        message.getString("timeGwRes") + "||" +
                        message.getString("timeThirdReq") + "||" +
                        message.getString("timeThirdRes") + "||" +
                        message.getString("resCode") + "||" +
                        message.getString("urlIn") + "||" +
                        message.getString("urlOut");
                fileWriter.write(keyValues + "\n");
            }
            log.info("消息写入文件成功！获取数据{}条，耗时{}ms", consumerRecords.size(),System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("消息写入文件失败。");
        }
        ack.acknowledge();
    }

    private String extractKeyFieldValues(String message) {
        Map<String, String> fieldValuePairs = new HashMap<>();
        String[] values = message.split("\\|\\|");
        for (int i = 0; i < fieldNames.length; i++) {
            fieldValuePairs.put(fieldNames[i], values[i]);
        }
        return fieldValuePairs.get("$remote_addr") + "||" +
               fieldValuePairs.get("$upstream_addr") + "||" +
               fieldValuePairs.get("$time_local") + "||" +
               fieldValuePairs.get("$request_time") + "||" +
               fieldValuePairs.get("$request") + "||" +
               fieldValuePairs.get("$status") + "||" +
               fieldValuePairs.get("$http_AppKey");
    }

    public void batchConsumer(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        List<TywlApigwLog> tywlApigwLogList = new ArrayList<>();
        List<TywlApigwLogAggregation> groupMapList  = new ArrayList<>();
        for (ConsumerRecord<?, ?> consumerRecord : consumerRecords) {
            //log.info("消费的每条数据为：{}", consumerRecord.value());
            TywlApigwLog tywlApigwLog = new TywlApigwLog();
            try {
                tywlApigwLog =  JSON.parseObject(consumerRecord.value().toString(),TywlApigwLog.class);
            }catch (Exception e){
                continue;
            }
            if (tywlApigwLog!=null && tywlApigwLog.getApiCode()!=null) {
                tywlApigwLogList.add(tywlApigwLog);
            }
        }
        //log.info("本次消费的数据量：{}", tywlApigwLogList.size());
        if (tywlApigwLogList!=null && tywlApigwLogList.size()>0) {
            Map<String, List<TywlApigwLog>> mapListGroupByName = tywlApigwLogList.stream().collect(Collectors.groupingBy(p ->
                    p.getAccount() + "#" + p.getGwErrorCode() + "#" + Arrays.toString(p.getTags()) + "#" + p.getApiCode() + "#" + p.getApiName() + "#" +
                            p.getAppCode() + "#" + p.getAppName() + "#" + p.getResCode() + "#" + "null" + "#" + "null"
                            + "#" + "null" + "#" + p.getUserName() + "#" + p.getUserId()
                            + "#" + p.getTimeGwReq().substring(0, 13) + ":00:00"
            ));
            //log.info("分组后：{}"+JSON.toJSONString(mapListGroupByName));

            // 对分组数据进行求和操作
            mapListGroupByName.forEach((groupName, mapByNameList) -> {
                //log.info("groupName:{}"+groupName);
                TywlApigwLogAggregation al = new TywlApigwLogAggregation();
                Long sumTimeGwDuration = mapByNameList.stream().mapToLong(map -> map.getTimeGwDuration()).sum();
                Long sumTimeThirdDuration = mapByNameList.stream().mapToLong(map -> map.getTimeThirdDuration()).sum();
                Long totalNum = mapByNameList.stream().mapToLong(map -> map.getTimeGwDuration()).count();
                OptionalLong maxTimeGwDuration = mapByNameList.stream().mapToLong(map -> map.getTimeGwDuration()).max();
                OptionalLong maxTimeThirdDuration = mapByNameList.stream().mapToLong(map -> map.getTimeThirdDuration()).max();
                OptionalLong minTimeGwDuration = mapByNameList.stream().mapToLong(map -> map.getTimeGwDuration()).min();
                OptionalLong minTimeThirdDuration = mapByNameList.stream().mapToLong(map -> map.getTimeThirdDuration()).min();
                OptionalDouble avgTimeGwDuration = mapByNameList.stream().mapToLong(map -> map.getTimeGwDuration()).average();
                OptionalDouble avgTimeThirdDuration = mapByNameList.stream().mapToLong(map -> map.getTimeThirdDuration()).average();

                if (!groupName.split("#")[0].equals("null")) {
                    al.setUserAccount(groupName.split("#")[0]);
                }
                if (!groupName.split("#")[1].equals("null")) {
                    al.setGwErrorCode(groupName.split("#")[1]);
                }

                if (!groupName.split("#")[2].equals("null")) {
                    al.setClusterTags(groupName.split("#")[2].replace("[", "").replace("]", "").split(","));
                }

                if (!groupName.split("#")[3].equals("null")) {
                    al.setApiCode(groupName.split("#")[3]);
                }
                if (!groupName.split("#")[4].equals("null")) {
                    al.setApiName(groupName.split("#")[4]);
                }
                if (!groupName.split("#")[5].equals("null")) {
                    al.setAppCode(groupName.split("#")[5]);
                }
                if (!groupName.split("#")[6].equals("null")) {
                    al.setAppName(groupName.split("#")[6]);
                }
                if (!groupName.split("#")[7].equals("null")) {
                    Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
                    if (pattern.matcher(groupName.split("#")[7]).matches()) {
                        al.setResCode(Integer.parseInt(groupName.split("#")[7]));
                    }
                }
                if (!groupName.split("#")[8].equals("null")) {
                    al.setCatalog1Code(groupName.split("#")[8]);
                }
                if (!groupName.split("#")[9].equals("null")) {
                    al.setCatalog1Name(groupName.split("#")[9]);
                }
                if (!groupName.split("#")[10].equals("null")) {
                    al.setRemoteIp(groupName.split("#")[10]);
                }
                if (!groupName.split("#")[11].equals("null")) {
                    al.setUserName(groupName.split("#")[11]);
                }
                if (!groupName.split("#")[12].equals("null")) {
                    al.setUserId(groupName.split("#")[12]);
                }
                if (!groupName.split("#")[13].equals("null")) {
                    al.setDealTime(groupName.split("#")[13]);
                }

                al.setTotalNum(totalNum);
                al.setSumTimeGwDuration(sumTimeGwDuration);
                al.setSumTimeThirdDuration(sumTimeThirdDuration);
                al.setMaxTimeGwDuration(maxTimeGwDuration.getAsLong());
                al.setMaxTimeThirdDuration(maxTimeThirdDuration.getAsLong());
                al.setMinTimeGwDuration(minTimeGwDuration.getAsLong());
                al.setMinTimeThirdDuration(minTimeThirdDuration.getAsLong());
                al.setAvgTimeGwDuration(avgTimeGwDuration.getAsDouble());
                al.setAvgTimeThirdDuration(avgTimeThirdDuration.getAsDouble());
                groupMapList.add(al);

            });

            // TODO 这边先不要打开，可以自己写入本地文件，上面的逻辑也可以先不要
//            if (groupMapList != null && groupMapList.size() > 0) {
//                for (TywlApigwLogAggregation tywlApigwLogAggregation : groupMapList) {
//                    kafkaTemplate.send("accout-aggregate", JSON.toJSONString(tywlApigwLogAggregation)).addCallback(success -> {
//                        // 消息在分区内的offset
//                        long offset = success.getRecordMetadata().offset();
//                        //log.info("发送消息到kafka队列成功:{}, offset为:{}", JSON.toJSONString(tywlApigwLogAggregation), offset);
//                    }, failure -> {
//                        log.error("发送消息到kafka队列失败:{}, 报错信息为:{}", JSON.toJSONString(tywlApigwLogAggregation), failure.getMessage());
//                    });
//                }
//            }
        }
        //手动提交
        ack.acknowledge();
        //log.info("收到kafka的数据，拉取数据量：{}，消费时间：{}ms", consumerRecords.size(), (System.currentTimeMillis() - start));
    }
}
