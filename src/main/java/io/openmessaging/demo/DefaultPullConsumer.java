package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 按照题目描述，每个线程单独起一个consumer，所以每个consumer应该不用考虑并发
 * 5/28 重构：采用push架构
 */
public class DefaultPullConsumer implements PullConsumer {    
    private KeyValue properties;
    private String queue;
    // buckets里面存的是该consumer绑定到的queue_name和订阅的topics
    private Set<Integer> buckets;   
    private MessageLoad messageLoad = null;
    // 当前消费者的编号
    private final Integer consumerNo;
    // 用来生成全局的消费者编号
    private static final AtomicInteger globalConsumerNo = new AtomicInteger(0);    
    private final String STORE_PATH; 
    
    // 5/28更新：用于接收分发的消息
    private ConcurrentLinkedQueue<DefaultBytesMessage> msgQue;
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        this.messageLoad = MessageLoad.getInstance();
        this.messageLoad.setStorePath(this.properties.getString("STORE_PATH"));
        this.STORE_PATH = this.properties.getString("STORE_PATH");
        this.consumerNo = globalConsumerNo.getAndIncrement();
        this.msgQue = new ConcurrentLinkedQueue<>();
        this.buckets = new HashSet<>(100);
//        // for test
//        if(this.consumerNo == 1) {
//        	logFileInfo();
//        }
    }
    
    public Integer getConsumerNO() {
    	return this.consumerNo;
    }
    
    // 统计文件数据
    private void logFileInfo() {
    	MyLogger.listAllFiles(this.STORE_PATH);
    }
    @Override public KeyValue properties() {
        return properties;
    }
      
    public void notifyMsg(final DefaultBytesMessage msg) {
    	this.msgQue.offer(msg);
    }
    
    @Override public void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        this.buckets.add(MyUtil.getTopicNo(queue));
        for(final String name : topics) {
        	this.buckets.add(MyUtil.getTopicNo(name));
        }
        messageLoad.registerConsumer(this, buckets);
    }
    // 该消费者是否已经消费完自己文件的消息
    private boolean pollFinished = false;
    // 是否已经设置过优先级。
    private boolean hasSetPriority = false;
    @Override public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        DefaultBytesMessage msg = null;
        while(msg == null) {
        	msg = this.msgQue.poll();        	
        	if(msg == null) {
        		if(!pollFinished) {
        			// 如果自己的队列是空，而且没有读完消息文件，就去读一下
        			this.pollFinished = this.messageLoad.readMsg(consumerNo);
        		}else{
        			if(!hasSetPriority) {
        				int normalPriority = Thread.NORM_PRIORITY;
        				// 如果自己的文件读完了，降低优先级
        				Thread.currentThread().setPriority(normalPriority-2);
        				this.hasSetPriority = true;
        			}
        		}
        	}        	
        }
        if(msg.getEndMsg()) {
        	return null;
        }
        return msg;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }  

}
