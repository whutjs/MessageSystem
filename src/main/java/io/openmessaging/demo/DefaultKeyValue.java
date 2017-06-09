package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
 * 编码方式：
 * 5/23 更新：value长度变长表示,key长度不会超过1byte
 * KEY_VALUE_BEGIN (1byte)+
 * 		KEY_LEN( 1byte) + key + 
 * 			V_INT_TYPE|V_LONG_TYPE|V_DOUBLE_TYPE|
 * 			(V_STRING_TYPE + LEN_TYPE + VALUE_LEN(byte|short|int)) +
 * 		value 
 */
/**
 * 6/2 更新：取消KEY_VALUE_BEGIN头部
 * KEY_LEN( 1byte) + key + 
 * 			(V_INT_TYPE|V_LONG_TYPE|V_DOUBLE_TYPE| 取消,全部都是字符串类型)
 * 			(V_STRING_TYPE + LEN_TYPE + VALUE_LEN(byte|short|int)) +
 * 		value 
 */
public class DefaultKeyValue implements KeyValue {

    private final Map<String, Object> kvs = new HashMap<>();
    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return (Integer)kvs.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return (Long)kvs.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return (Double)kvs.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String)kvs.getOrDefault(key, null);
    }
    
    /**
     * 用于不知道key对应于什么类型的情况
     * @param key
     * @return
     */
    public Object get(final String key) {
    	return kvs.getOrDefault(key, null);
    }
    
    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
    
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append('[');
    	for (Map.Entry<String, Object> entry : kvs.entrySet())
    	{    		
    		String key = entry.getKey();    		
    		Object value = entry.getValue();
    		sb.append(key).append(':').append(value.toString()).append(", ");
    		
    	}
    	sb.append(']');
    	return sb.toString();
    }    
}
