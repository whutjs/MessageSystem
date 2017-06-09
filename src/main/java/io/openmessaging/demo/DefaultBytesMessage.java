package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;


/**
 * 6/2 更新：header部分只存messageId的value，key都不用存；Topic也不用存！
 * Property的key：PRO_OFFSET 用1byte表示！
 * 一条消息编码为：
 * 头部：MSG_HEADER_BEGIN + MSG_ID_LEN(Short)+ MSG_ID
 * Property: (KEY_LEN(byte) + key_str + VALUE_LEN (Short) + VALUE_STR)
 * 			 ....
 * MSG_BODY: MSG_BODY_BEGIN (-3) + MSG_BODY_LEN (Short) + MSG_BODY
 */
public class DefaultBytesMessage implements BytesMessage {
	
    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;
    // 表示当前消息是不是表示结束的消息
    private boolean endMsg;
    public void setEndMsg(boolean flag) {
    	this.endMsg = flag;
    }
    public boolean getEndMsg() {
    	return this.endMsg;
    }
    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }
    
    public void setHeader(DefaultKeyValue hkv) {
    	this.headers = hkv;
    }
    
    public void setProperty(DefaultKeyValue pkv) {
    	this.properties = pkv;
    }
    
    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {    	
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }
    
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("{ headers= ").append(headers.toString()).append(", ");
    	if(properties != null) {
    		sb.append("properties= ").append(properties.toString()).append(", ");
    	}
    	sb.append("body= ").append(new String(body));
    	sb.append(" }");
    	return sb.toString();
    }
        
}
