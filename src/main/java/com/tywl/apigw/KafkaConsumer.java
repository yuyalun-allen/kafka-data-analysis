package com.tywl.apigw;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@Slf4j
public class KafkaConsumer implements ConsumerSeekAware {
    @Value("${kafka.consumer.output-base-dir}")
    private String BASE_PATH;

    @Value("${kafka.consumer.output-inter-minutes}")
    private long OUTPUT_INTERVAL_MIN;

    private static final Object southNginxWriteLock = new Object();
    private static final Object northNginxWriteLock = new Object();
    private static final Object southAppWriteLock = new Object();
    private static final Object northAppWriteLock = new Object();

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

    /** 以下4个Listener分别监听4个不同的主题。 */

    @KafkaListener(topics = {"south-nginx"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void southNginxExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        synchronized (southNginxWriteLock) {
            writeToCsv(consumerRecords, "south-nginx", start);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"north-nginx"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void northNginxExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        synchronized (northNginxWriteLock) {
            writeToCsv(consumerRecords, "north-nginx", start);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"north-app"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void northAppExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        synchronized (northAppWriteLock) {
            writeToCsv(consumerRecords, "north-app", start);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = {"south-app"}, containerFactory = "batchFactory", errorHandler="consumerAwareErrorHandler")
    public void southAppExtraction(List<ConsumerRecord<?, ?>> consumerRecords, Acknowledgment ack) {
        long start = System.currentTimeMillis();
        synchronized (southAppWriteLock) {
            writeToCsv(consumerRecords, "south-app", start);
        }
        ack.acknowledge();
    }

    private void writeToCsv(List<ConsumerRecord<?, ?>> consumerRecords, String topic, long start) {
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(determineAndCreateOutputDir() + topic + "-message.csv", true))) {
            // 处理app数据
            if (topic.split("-")[1].equals("app")) {
                for (ConsumerRecord<?, ?> record : consumerRecords) {
                    JSONObject message = JSON.parseObject(record.value().toString());
                    String type = message.getString("logType");
                    if (type == null || !type.equals("billing")) {
                        continue;
                    }
                    String keyValues = extractKeyFieldValues(message) + "\n";
                    fileWriter.write(keyValues);
                }
            // 处理nginx数据
            } else if (topic.split("-")[1].equals("nginx")) {
                for (ConsumerRecord<?, ?> record : consumerRecords) {
                    String message = JSON.parseObject(record.value().toString()).getString("message");
                    String messageRecord = extractKeyFieldValues(message) + "\n";
                    fileWriter.write(messageRecord);
                }
            } else {
                log.error("------------------不合法的Topic！--------------------");
            }
            log.info("{}: 消息写入文件成功！获取数据{}条，耗时{}ms",topic, consumerRecords.size(), System.currentTimeMillis() - start);
        } catch (IOException ioe) {
            log.error("{}: 消息写入文件失败。", topic);
        }
    }

    private String extractKeyFieldValues(String message) {
        Map<String, String> fieldValuePairs = new HashMap<>();
        String[] values = message.split("\\|\\|");
        for (int i = 0; i < fieldNames.length; i++) {
            fieldValuePairs.put(fieldNames[i], values[i]);
        }
        return fieldValuePairs.get("$remote_addr") + "," +
               fieldValuePairs.get("$time_local") + "," +
               fieldValuePairs.get("$upstream_addr") + "," +
               fieldValuePairs.get("$request_time") + "," +
               fieldValuePairs.get("$status") + "," +
               fieldValuePairs.get("$request").split(" ")[1].split("\\?")[0].trim() + "," +
               fieldValuePairs.get("$http_AppKey").trim();
    }

    private String extractKeyFieldValues(JSONObject message) {
        return message.getString("apiCode") + "," +
               message.getString("apiName") + "," +
               message.getString("appCode") + "," +
               message.getString("appName") + "," +
               message.getString("urlIn") + "," +
               message.getString("urlOut") + "," +
               message.getString("date") + "," +
               message.getString("timeGwReq") + "," +
               message.getString("timeGwRes") + "," +
               message.getString("timeThirdReq") + "," +
               message.getString("timeThirdRes") + "," +
               message.getString("timeThirdDuration") + "," +
               message.getString("timeGwDuration") + "," +
               message.getString("resCode") + "," +
               message.getString("userId") + "," +
               message.getString("account");
    }

    private String determineAndCreateOutputDir() {
        long currentTime = System.currentTimeMillis();
        long outputIntervalMs = OUTPUT_INTERVAL_MIN * 1000 * 60;
        long roundedTime = (currentTime / outputIntervalMs) * outputIntervalMs;

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        String timestamp = dateFormat.format(new Date(roundedTime));
        String outputDir = BASE_PATH + timestamp + "/";
        try {
            Files.createDirectories(Paths.get(outputDir));
        } catch (IOException e) {
            log.error("创建文件夹" + outputDir + "失败");
        }
        return outputDir;
    }
}
