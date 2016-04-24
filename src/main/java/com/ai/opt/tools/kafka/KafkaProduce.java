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
import java.util.List;

/**
 * Created by jackieliu on 16/3/24.
 */
public class KafkaProduce {
    private static final String VAL_FILE_NAME = "vals.txt";

    /**
     * 发送从文件读取的内容
     */
    public void sendMsgFromFile(){
        if (KafkaConf.VAL_TYPE_JSON.equals(KafkaConf.getValType())){
            sendJsonMsgFromFile();
        }else
            sendStrMsgFromFile();
    }
    /**
     * 从vals.json文件中读取json内容,发送至kafka
     */
    private void sendJsonMsgFromFile(){
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

    /**
     * 从文件中读取字符串内容
     */
    private void sendStrMsgFromFile(){
        Producer<String,String> producer = new KafkaProducer<String,String>(
                KafkaConf.getProps(),KafkaConf.getKeySerializer(),KafkaConf.getValueSerializer());
        try {
            List<String> valList = getValsStrArray();
            for (String jsonObject: valList){
                producer.send(
                        new ProducerRecord<String, String>(KafkaConf.getTopicName(), jsonObject));
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    /**
     * 从文件中读取字符串内容,发送至kafka
     */
    private List<String> getValsStrArray() throws URISyntaxException,IOException {
        URL valsUrl = KafkaProduce.class.getClassLoader().getResource(VAL_FILE_NAME);
        return FileUtils.readLines(new File(valsUrl.toURI()));
    }

    public void sendStrMsg(String msg){
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
        URL valsUrl = KafkaProduce.class.getClassLoader().getResource(VAL_FILE_NAME);
        byte[] valBytes = FileUtils.readFileToByteArray(new File(valsUrl.toURI()));
        return  (JSONArray)JSON.parse(valBytes);
    }

    public static void main(String[] args) throws ConfigException {
        KafkaProduce kafkaProduce = new KafkaProduce();
        KafkaConf.loadConf();
        kafkaProduce.sendJsonMsgFromFile();
    }
}
