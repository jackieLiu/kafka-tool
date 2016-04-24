package com.ai.opt.tools.kafka;

import com.ai.opt.tools.kafka.exception.ConfigException;
import com.ai.opt.tools.kafka.util.KafkaConf;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by jackieliu on 16/4/11.
 */
public class KafkaProduceTest {
    KafkaProduce kafkaProduce;

    @Before
    public void initBeans() throws ConfigException {
        kafkaProduce = new KafkaProduce();
        KafkaConf.loadConf();
    }

    @Test
    public void loadConfTest(){
        Properties confProperties = KafkaConf.getProps();
        System.out.println(confProperties.toString());
        Assert.assertEquals(confProperties.size(),7);
    }

    @Test
    public void sendMsgFromFileTest(){
        kafkaProduce.sendMsgFromFile();
    }

    /**
     * 模拟停止操作
     */
    @Test
    public void sendOneMsg(){
        //信控数据源信息
        JSONObject jsonObject = new JSONObject();
        //数量
        jsonObject.put("amount", "30");
        //acct:账户；cust:客户；subs:用户
        jsonObject.put("owner_type", "subs");
        //属主id
        jsonObject.put("owner_id", "122301");
        //数量的类型 VOICE:语音资源;DATA:流量资源;SM:短信资源;VC:虚拟币资源;BOOK:资金账本;PC:电量资源
        jsonObject.put("amount_type", "DATA");
        //事件类型：CASH主业务（按资料信控），VOICE 语音，SMS 短信，DATA 数据
        jsonObject.put("event_type", "CASH");
        //数量的增减属性，包括PLUS(导致余额增加的，如缴费导致的)，MINUS(导致余额减少的，如业务使用导致的)
        jsonObject.put("amount_mark", "MINUS");
        //来源 resource：资源入账，bmc：计费
        jsonObject.put("source_type", "bmc");
        //租户id
        jsonObject.put("tenant_id", "VIV-BYD");
        //系统id
        jsonObject.put("system_id", "VIV");
        //事件id
        jsonObject.put("event_id", Long.toString(System.currentTimeMillis()));
        //扩展信息，用json传递具体信息
        jsonObject.put("expanded_info", "");
        System.out.println(jsonObject.toString());
        kafkaProduce.sendMsg(jsonObject.toString());
    }
}
