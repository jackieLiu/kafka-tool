package com.ai.opt.tools.kafka;

import com.ai.opt.tools.kafka.exception.ConfigException;
import com.ai.opt.tools.kafka.util.KafkaConf;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by jackieliu on 16/3/24.
 */
public class KafkaProduce {
    /**
     * 从vals.json文件中读取json内容,发送至kafka
     */
    public void sendMsgFromFile(){
        Producer<String,String> producer = new KafkaProducer<String,String>(
                KafkaConf.getProps(),KafkaConf.getKeySerializer(),KafkaConf.getValueSerializer());
        try {
            JSONArray jsonArray = getValsJsonArray();
            for (Object jsonObject:jsonArray){
                producer.send(
                new ProducerRecord<String, String>(KafkaConf.getTopicName(), jsonObject.toString()));
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public void sendMsg(String msg){
        Producer<String,String> producer = new KafkaProducer<String,String>(
                KafkaConf.getProps(),KafkaConf.getKeySerializer(),KafkaConf.getValueSerializer());
        producer.send(
                new ProducerRecord<String, String>(KafkaConf.getTopicName(), msg));
        producer.close();
    }

    /**
     * 将文本内容转换为json集合
     * @return
     */
    private JSONArray getValsJsonArray() throws URISyntaxException, IOException {
        URL valsUrl = KafkaProduce.class.getClassLoader().getResource("vals.json");
        byte[] valBytes = FileUtils.readFileToByteArray(new File(valsUrl.toURI()));
        return  (JSONArray)JSON.parse(valBytes);
    }

    public static void main(String[] args) throws ConfigException {
        KafkaProduce kafkaProduce = new KafkaProduce();
        KafkaConf.loadConf();
        kafkaProduce.sendMsgFromFile();
    }
}
