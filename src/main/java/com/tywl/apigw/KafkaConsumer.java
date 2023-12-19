package com.tywl.apigw;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Component
@Slf4j
public class KafkaConsumer implements ConsumerSeekAware {
    private static final String[] fieldNames;
    static {
        fieldNames = "$remote_addr||$remote_user||$time_local||$http_x_forwarded_for||$http_true_client_ip||$upstream_addr||$upstream_response_time||$request_time||$hostname||$host||$http_host||$request||$status||$body_bytes_sent||$http_referer||$http_user_agent||$http_AppKey"
                    .split("\\|\\|");
    }

    /** 每次客户端分配到Topic分区时都从最新的offset开始。*/
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
                              ConsumerSeekCallback callback) {
        callback.seekToEnd(assignments.keySet());
    }

    @KafkaListener(topics = {"south-nginx"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void southNginxExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        try (FileWriter fileWriter = new FileWriter("south-nginx-message.txt", true)) {
            for (ConsumerRecord<?, ?> record: consumerRecords) {
                String message = JSON.parseObject(record.value().toString()).getString("message");
                fileWriter.write(extractKeyFieldValues(message) + "\n");
            }
            log.info("south-nginx: 消息写入文件成功！获取数据{}条，耗时{}ms", consumerRecords.size(),System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("south-nginx: 消息写入文件失败。");
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"north-nginx"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void northNginxExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        try (FileWriter fileWriter = new FileWriter("north-nginx-message.txt", true)) {
            for (ConsumerRecord<?, ?> record: consumerRecords) {
                String message = JSON.parseObject(record.value().toString()).getString("message");
                fileWriter.write(extractKeyFieldValues(message) + "\n");
            }
            log.info("north-nginx: 消息写入文件成功！获取数据{}条，耗时{}ms", consumerRecords.size(),System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("north-nginx: 消息写入文件失败。");
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"north-app"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void northAppExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        try (FileWriter fileWriter = new FileWriter("north-app-message.txt", true)) {
            for (ConsumerRecord<?, ?> record: consumerRecords) {
                JSONObject message = JSON.parseObject(record.value().toString());
                String type = message.getString("logType");
                if (type == null || !type.equals("billing")) {
                    continue;
                }
                String keyValues = extractKeyFieldValues(message);
                fileWriter.write(keyValues + "\n");
            }
            log.info("north-app: 消息写入文件成功！获取数据{}条，耗时{}ms", consumerRecords.size(),System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("north-app: 消息写入文件失败。");
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"south-app"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void southAppExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        try (FileWriter fileWriter = new FileWriter("south-app-message.txt", true)) {
            for (ConsumerRecord<?, ?> record: consumerRecords) {
                JSONObject message = JSON.parseObject(record.value().toString());
                String type = message.getString("logType");
                if (type == null || !type.equals("billing")) {
                    continue;
                }
                String keyValues = extractKeyFieldValues(message);
                fileWriter.write(keyValues + "\n");
            }
            log.info("south-app: 消息写入文件成功！获取数据{}条，耗时{}ms", consumerRecords.size(),System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("south-app: 消息写入文件失败。");
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
               fieldValuePairs.get("$time_local") + "||" +
               fieldValuePairs.get("$upstream_addr") + "||" +
               fieldValuePairs.get("$request_time") + "||" +
               fieldValuePairs.get("$status") + "||" +
               fieldValuePairs.get("$request") + "||" +
               fieldValuePairs.get("$http_AppKey");
    }

    private String extractKeyFieldValues(JSONObject message) {
        return message.getString("apiCode") + "||" +
               message.getString("apiName") + "||" +
               message.getString("appCode") + "||" +
               message.getString("appName") + "||" +
               message.getString("urlIn") + "||" +
               message.getString("urlOut") + "||" +
               message.getString("date") + "||" +
               message.getString("timeGwReq") + "||" +
               message.getString("timeGwRes") + "||" +
               message.getString("timeThirdReq") + "||" +
               message.getString("timeThirdRes") + "||" +
               message.getString("timeThirdDuration") + "||" +
               message.getString("timeGwDuration") + "||" +
               message.getString("resCode") + "||" +
               message.getString("userId") + "||" +
               message.getString("account");
    }
}
