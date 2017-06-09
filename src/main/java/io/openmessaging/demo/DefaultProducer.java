package io.openmessaging.demo;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

// 保存cache和对应的pos
class CachePos {
	byte[] cache;
	// 表示该cache当前写到了哪个位置
	int pos;
	public CachePos(int size) {
		this.cache = new byte[size];
		this.pos = 0;
	}
}

public class DefaultProducer  implements Producer {
	/** 6/2 改消息序列化方式：有些固定出现的字符串可以hardcode */
	// 用来统计数目
	private static AtomicInteger producerCnt = new AtomicInteger(0);
	private final int prodNo;
    private MessageFactory messageFactory = null;
    private KeyValue properties;
    // 用来保存编码后的消息
    private ByteBuffer encodedMsg;
    
    /** 5/26 更新：架构更改，每个producer维护对每个topic文件都维护一个
     	自己所写消息的cache
     */
    private static final int KB_2 = (1<<11);
    private static final int[] SIZE_ARRAY = {
//    	// 2KB  4KB    8KB       16KB      32KB     64KB    128KB
//    	KB_2, KB_2<<1, KB_2<<2, KB_2<<3, KB_2<<4, KB_2<<5, KB_2<<6,
    	// 64KB  4KB    8KB       16KB     32KB     64KB    128KB
    	KB_2<<5, KB_2<<1, KB_2<<2, KB_2<<3, KB_2<<4, KB_2<<5, KB_2<<6,
    	// 256KB   512KB       1024KB
    	 KB_2<<7,  KB_2<<8,  KB_2<<9
    };
    
    /** 5/31 更新：Chunk大小用8KB */
    private final int CACHE_SIZE;
    // 保存某个topic或者queue对应的Cache。cache中保存的是已经序列化好的消息byte[]
    private HashMap<String, CachePos> msgCacheMap;
 	
    /** 5/27架构大改：每个Producer将自己生产的消息写到一个文件去 */
    private FileWriteCache fileWriteCache;
    private final String storePath;
    
    public DefaultProducer(KeyValue properties) {
    	this.messageFactory = new DefaultMessageFactory();
        this.properties = properties;
        this.storePath = this.properties.getString("STORE_PATH");
        this.encodedMsg = ByteBuffer.allocate(Constant.MAX_MSG_SIZE);
        this.prodNo = producerCnt.getAndIncrement();
        this.CACHE_SIZE = SIZE_ARRAY[this.prodNo % SIZE_ARRAY.length];
        
        /** 5/26 更新：架构更改，每个producer维护对每个topic文件都维护一个
     		自己所写消息的cache
     		 5/30 更新：不用cache，直接序列化每一条消息然后写文件看看
         */
        this.msgCacheMap = new HashMap<>(100);
		try {
			this.fileWriteCache = new FileWriteCache(Paths.get(storePath, String.valueOf(this.prodNo)).toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    // 释放资源
    public void release() {
    	encodedMsg.clear();
    	encodedMsg = null;
    }
    
    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
    	// 每产生一条消息都要去看看这个topic有没有记录
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
    	// 每产生一条消息都要去看看这个queue有没有记录
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {
    }
    // 用来让生产者等待所有生产者生产消息完毕
    private static volatile CyclicBarrier barrier  = null;
    @Override public void flush() {
    	// 把自己的cache里面的数据全部压缩并写入FileWriteCache
    	for(final String name : this.msgCacheMap.keySet()) {
    		CachePos cp = this.msgCacheMap.get(name);
    		if(cp.pos == 0) continue;
    		compactAndWrite2File(name, cp.cache, cp.pos);
			cp.pos = 0;
    	}
    	this.msgCacheMap.clear();
    	this.fileWriteCache.flush();    
    	this.fileWriteCache.close();
    	if(barrier == null) {
    		synchronized (DefaultProducer.class){
                if(barrier == null){
                	MyLogger.log(MyLogger.INFO_TAG, "Producer cnt:" + producerCnt.get());
                	barrier = new CyclicBarrier(producerCnt.get());
                }
            }
    	}
    	try {
			barrier.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}    	
    	if(this.prodNo == 0) {
    		saveMetaData();
    	}
    }
    
    private void saveMetaData() {
    	// 保存topicNo还有生产者编号映射
    	BufferedWriter writer = null;
    	File file = Paths.get(this.storePath, Constant.TOPIC_NO_FILE).toFile();
    	try
    	{
    	    writer = new BufferedWriter( new FileWriter(file));
    	    ConcurrentHashMap<String, Integer> topicNoMap = MyUtil.getTopicNoMap();
    	    for(final String name : topicNoMap.keySet()) {
    	    	Integer topicNo = topicNoMap.get(name);
    	    	StringBuilder sb = new StringBuilder();
    	    	sb.append(name).append(':').append(topicNo);    	    	
    	    	writer.write(sb.toString());
    	    	writer.newLine();
    	    }
    	}
    	catch ( IOException e)
    	{
    	}
    	finally
    	{
    	    try
    	    {
    	        if ( writer != null)
    	        writer.close( );
    	    }
    	    catch ( IOException e)
    	    {
    	    }
    	}
    }
    
    @Override public KeyValue properties() {
        return properties;
    }
    
    
    
    /* 编码消息在Producer线程进行 */
    @Override public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        String name = (topic == null? queue:topic);
        encodeMsg((DefaultBytesMessage)message, name); 
        encodedMsg.flip();        
        int dataLen = encodedMsg.remaining();
        write2OwnCache(name, encodedMsg.array(), dataLen);
    }
    
    private void compactAndWrite2File(final String name, final byte[] data, final int dataLen) {
    	/** 6/3更新：不压缩，直接写 */
    	// topicNo:
    	int topicNo = MyUtil.getTopicNo(name);
    	// 写入FileWriteCache
    	this.fileWriteCache.write(topicNo, data, dataLen);
    }
    
    private void write2OwnCache(final String name, final byte[] data, final int dataLen) {
    	// 延迟初始化cachePos:只有到写入的才初始化
    	if(!this.msgCacheMap.containsKey(name)) {
    		// 不需要double check来防止多次初始化，因为一个生产者只在一条线程执行
    		this.msgCacheMap.put(name, new CachePos(CACHE_SIZE));
    	} 
    	/** 5/31 Update:用64KB作为chunk大小，那么如果某条消息超过chunk大小，先把
    	 * chunk的内容写到FileWriteCache，再单独把这条大消息压缩并写入FileWriteCache
    	 */
    	/**
    	 * 6/3更新：试试不用压缩
    	 */
    	CachePos cp = this.msgCacheMap.get(name);
    	if(cp.pos + dataLen > CACHE_SIZE) {
    		if(cp.pos > 0) {
    			compactAndWrite2File(name, cp.cache, cp.pos);			
    			cp.pos = 0;
    		}
		}
		
    	if(dataLen > CACHE_SIZE) {
    		// 是大消息，直接压缩并写入
    		compactAndWrite2File(name, data, dataLen);	
    	}else{
    		System.arraycopy(data, 0, cp.cache, cp.pos, dataLen);
    		cp.pos += dataLen;
    	}
    }
        
    
    
    private void encodeMsg(final DefaultBytesMessage message, final String topic) {
    	/**
    	 * 6/2 更新：header部分只存messageId的value，key都不用存；Topic也不用存！
    	 * Property的key：PRO_OFFSET 用1byte表示！
    	 * 一条消息编码为：
    	 * 头部：MSG_HEADER_BEGIN + MSG_ID_LEN(Short)+ MSG_ID
    	 * Property: (KEY_LEN(byte) + key_str + VALUE_LEN (Short) + VALUE_STR)
    	 * 			 ....
    	 * MSG_BODY: MSG_BODY_BEGIN (-3) + MSG_BODY_LEN (Short) + MSG_BODY
    	 */
    	encodedMsg.clear();      
        encodeMsgBody(encodedMsg, message, topic);
    }
    
    /*
     *  编码头部
     */
    private void encodeMsgHeader(final ByteBuffer encodeMsg, final DefaultBytesMessage message) {
    	// 头部只存了MSG_ID: len(short) + value
    	DefaultKeyValue header = (DefaultKeyValue) message.headers();
    	String msgIdVal = (String)header.get(Constant.MSGID_KEY);
    	byte[] valueBytes = msgIdVal.getBytes();
    	int valueBytesLen = valueBytes.length;
    	encodedMsg.putShort((short)valueBytesLen);
    	encodedMsg.put(valueBytes);
    }
    
    private void encodeMsgBody(final ByteBuffer encodeMsg, 
    		final DefaultBytesMessage message, final String topicName) {
    	/**
    	 * 6/2 更新：header部分只存messageId的value，key都不用存；Topic也不用存！
    	 * Property的key：PRO_OFFSET 用1byte表示！
    	 * 一条消息编码为：
    	 * 头部：MSG_HEADER_BEGIN + MSG_ID_LEN(Short)+ MSG_ID
    	 * Property: (KEY_LEN(byte) + key_str + VALUE_LEN (Short) + VALUE_STR)
    	 * 			 ....
    	 * MSG_BODY: MSG_BODY_BEGIN (-3) + MSG_BODY_LEN (Short) + MSG_BODY
    	 */
        // 消息头开始标识
    	encodeMsg.put(Constant.MSG_HEADER_BEGIN);        
        // 编码头部
    	encodeMsgHeader(encodeMsg, message);
        DefaultKeyValue property = (DefaultKeyValue)message.properties();
        if(property != null) {
            encodeKeyValue(encodeMsg, property, topicName);
        }
        // 消息body部分
        encodeMsg.put(Constant.MSG_BODY_BEGIN);
        int msgBodyLen = message.getBody().length;
        encodeMsg.putShort((short)msgBodyLen);
        encodeMsg.put(message.getBody());        
    }
    
    private void encodeKeyValue(final ByteBuffer encodedMsg, 
    		final KeyValue keyValue, final String topicName) {
    	DefaultKeyValue kv = (DefaultKeyValue)keyValue;    
    	/**
    	 * 6/3 更新：header部分只存messageId的value，key都不用存；Topic也不用存！
    	 * Property的key：PRO_OFFSET 用1byte表示！
    	 * 一条消息编码为：
    	 * 头部：MSG_HEADER_BEGIN + MSG_ID_LEN(Short)+ MSG_ID
    	 * Property: (KEY_LEN(byte) + key_str + VALUE_LEN (Short) + VALUE_STR)
    	 * 			 ....
    	 * MSG_BODY: MSG_BODY_BEGIN (-3) + MSG_BODY_LEN (Short) + MSG_BODY
    	 */
    	for (final String key : kv.keySet())
    	{
    		    		  	
    		Object value = kv.get(key);
    		if(key.length() > 2 && key.charAt(0) == 'P' && key.charAt(1) == 'R') {
    			continue;
    		}else{
	    		// 需要1 byte标识key的长度
	    		byte[] keyBytes = key.getBytes();
	    		// 5/22更新：仍旧用1byte
	    		byte keyLen = (byte) keyBytes.length;
	    		//int keyLen = keyBytes.length;
	    		encodedMsg.put(keyLen);
	    		encodedMsg.put(keyBytes);
	    		byte[] valueBytes = ((String)value).getBytes();
		    	int valueBytesLen = valueBytes.length;
		    	encodedMsg.putShort((short)valueBytesLen);
		    	encodedMsg.put(valueBytes);    		
    		}
    	}
    }
    
    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}