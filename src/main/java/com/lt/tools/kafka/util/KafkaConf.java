package com.lt.tools.kafka.util;

import com.lt.tools.kafka.KafkaProduce;
import com.lt.tools.kafka.exception.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 用于保存kafka的配置信息
 * @author jackieliu
 *
 */
public class KafkaConf {
    private static Properties props;
    /**
     * 发送值得类型
     */
    private static String VAL_TYPE;
    //字符串类型
    public static final String VAL_TYPE_STRING = "string";
    //json类型
    public static final String VAL_TYPE_JSON = "json";
    private static String TOPIC_NAME;
    //key序列化方式
    private static Serializer KEY_SERIALIZER = new StringSerializer();
    //值序列化方式
    private static Serializer VALUE_SERIALIZER = new StringSerializer();

    public static String getTopicName() {
        return TOPIC_NAME;
    }

    public static Properties getProps() {
        return props;
    }

    public static Serializer getKeySerializer() {
        return KEY_SERIALIZER;
    }

    public static Serializer getValueSerializer() {
        return VALUE_SERIALIZER;
    }

    public static String getValType(){
        return VAL_TYPE;
    }

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
    public static void loadConf() throws ConfigException {
        if (props!=null && props.size()>0)
            return;
        try {
            props = new Properties();
            InputStream input = KafkaProduce.class.getClassLoader()
                    .getResourceAsStream("kafka/kafka.properties");
            props.load(input);
            TOPIC_NAME = props.getProperty("topic.name");
            if (TOPIC_NAME==null)
                throw new org.apache.kafka.common.config.ConfigException("The topic name is null.");
            //key序列化
//            org.apache.kafka.common.serialization.StringSerializer
            KEY_SERIALIZER = getSerializer(props.getProperty("key.serializer.type","string"));
            VALUE_SERIALIZER = getSerializer(props.getProperty("value.serializer.type","string"));
            //发送类型
            VAL_TYPE = props.getProperty("val.type","string");
            //删除无用配置
            props.remove("key.serializer.type");
            props.remove("value.serializer.type");
        } catch (IOException e) {
            throw new ConfigException("配置信息加载异常",e);
        }
    }

    private static Serializer getSerializer(String type){
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
                throw new org.apache.kafka.common.config.ConfigException("error key serializer type:"+type);
        }
        return serializer;
    }
}
