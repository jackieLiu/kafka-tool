package com.ai.opt.tools.kafka.exception;

/**
 * Created by jackieliu on 16/4/11.
 */
public class ConfigException extends Exception {
    public ConfigException(){
        super();
    }
    public ConfigException(String errMsg){
        super(errMsg);
    }
    public ConfigException(String errMsg,Throwable cause){
        super(errMsg,cause);
    }
}
