package com.ai.opt.tools.kafka;

import com.ai.opt.tools.kafka.exception.ConfigException;
import com.ai.opt.tools.kafka.util.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.*;

/**
 * Created by jackieliu on 16/3/28.
 */
public class KafkaConsumerTest {

    @Test
    public void consumerTest() throws Exception {
        KafkaConf.loadConf();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConf.getProps().getProperty("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS, 30000l);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY,"");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000l);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe("omckafka");
        boolean isRunning = true;
        while (isRunning) {
            Map<String, ConsumerRecords<String, String>> records = consumer.poll(100);
            System.out.println(records.size());
            Iterator<String> keySet = records.keySet().iterator();
            while (keySet.hasNext()){
                ConsumerRecords<String, String> consumerRecords = records.get(keySet.next());
                System.out.printf("recordKey = %s,topicName = %s \r\n",keySet.next(),consumerRecords.topic());
                for (ConsumerRecord<String, String> record:consumerRecords.records()) {
                    System.out.printf("==== offset = %d, key = %s, value = %s \r\n", record.offset(), record.key(), record.value());
                }
            }
        }
//        consumer.commit(true);
        consumer.close();
    }
}
