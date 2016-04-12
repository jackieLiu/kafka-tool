package com.ai.opt.tools.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * Created by jackieliu on 16/3/24.
 */
public class KafkaProduce {
    //与配置文件中kafka.spout.topic一致
    private static final String OMC_TOPIC = "omckafka";
    //kafka的topic名称
    private String topicName;
    Properties props = new Properties();
    //key序列化方式
    Serializer keySerializer = new StringSerializer();
    //值序列化方式
    Serializer valueSerializer = new StringSerializer();

    /**
     * 加载配置信息
     *
     * props.put("bootstrap.servers", "10.1.130.84:39091,10.1.130.85:39091,10.1.236.122:39091");
     * props.put("acks", "all");
     * props.put("retries", 0);
     * props.put("batch.size", 16384);
     * props.put("linger.ms", 1);
     * props.put("buffer.memory", 33554432);
     */
    public void loadConf(){
        if (props.size()>0)
            return;
        try {
            InputStream input = KafkaProduce.class.getClassLoader()
                    .getResourceAsStream("kafka/kafka.properties");
            props.load(input);
            topicName = props.getProperty("topic.name");
            if (topicName==null)
                throw new ConfigException("The topic name is null.");
            //key序列化
//            org.apache.kafka.common.serialization.StringSerializer
            keySerializer = getSerializer(props.getProperty("key.serializer.type","string"));
            valueSerializer = getSerializer(props.getProperty("value.serializer.type","string"));
            //删除无用配置
            props.remove("key.serializer.type");
            props.remove("value.serializer.type");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从vals.json文件中读取json内容,发送至kafka
     */
    public void sendMsgFromFile(){
        Producer<String,String> producer = new KafkaProducer<String,String>(props,keySerializer,valueSerializer);
        try {
            JSONArray jsonArray = getValsJsonArray();
            for (Object jsonObject:jsonArray){
                producer.send(
                new ProducerRecord<String, String>(topicName, jsonObject.toString()));
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
        Producer<String,String> producer = new KafkaProducer<String,String>(props,keySerializer,valueSerializer);
        producer.send(
                new ProducerRecord<String, String>(topicName, msg));
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


    private Serializer getSerializer(String type){
        if (type==null)
            return null;
        Serializer serializer = null;
        switch (type){
            case "string":
                serializer = new StringSerializer();
                break;
            case "byteArray":
                serializer = new ByteArraySerializer();
                break;
            default:
                throw new ConfigException("error key serializer type:"+type);
        }
        return serializer;
    }

    public static void main(String[] args){
        KafkaProduce kafkaProduce = new KafkaProduce();
        kafkaProduce.loadConf();
        kafkaProduce.sendMsgFromFile();
    }
}
