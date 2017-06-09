package io.openmessaging.demo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

/**
 * 单例类，给Consumer使用，用于统计每个topic有多少consumer订阅之类。
 * 5/28更新：采用push架构
 * @author Jenson
 */
public final class MessageLoad {

	private static volatile MessageLoad INSTANCE = null;

    public static MessageLoad getInstance() {
    	if(INSTANCE == null) {
    		synchronized (MessageStore.class){
                if(INSTANCE == null){
                	INSTANCE = new MessageLoad();
                	// 5/24 更新 打一个Log说明是最新版本代码
                	System.out.println("6/4 8:00更新的代码");
                }
            }
    	}
        return INSTANCE;
    }
    // 存储文件的路径
    private String filePath = null;
    // 保存topicNo对应的消费者
    private HashMap<Integer, ArrayList<DefaultPullConsumer>> topicConsumerMap;
    private HashSet<DefaultPullConsumer> consumerSet;
    // 保存每个消费者对应的FileReadCache
    private HashMap<Integer, FileReadCache> fileReadCacheMap;
    private final CountDownLatch latch;
    private MessageLoad() {    
    	this.topicConsumerMap = new HashMap<>(100);
    	this.fileReadCacheMap = new HashMap<>(Constant.GROUP_NUM);
    	this.latch = new CountDownLatch(Constant.GROUP_NUM);
    	this.consumerSet = new HashSet<>(Constant.GROUP_NUM);
    	new Thread(
    		()->{
    			waitAndSendEndMsg();
    		}
    	).start();
    }  
    // 等待所有消费者消费完毕然后发送结束消息
    private void waitAndSendEndMsg() {
    	try {
			this.latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	DefaultBytesMessage endMsg = new DefaultBytesMessage(null);
    	endMsg.setEndMsg(true);
    	for(final DefaultPullConsumer cs : this.consumerSet) {
    		cs.notifyMsg(endMsg);
    	}
    }
    
    public void setStorePath(final String path) {
    	if(filePath == null) {
    		synchronized (this) {
				if(filePath == null) {
					this.filePath = path;
					MyUtil.constructMetaData(path);
					for(int i = 0; i < Constant.GROUP_NUM; ++i) {
						this.fileReadCacheMap.put(i, new FileReadCache(i, Paths.get(path, String.valueOf(i)).toString()));
					}
					
				}
			}    		
    	}
    }
    
    // 如果读完消息了返回true,否则返回false
    public boolean readMsg(final Integer consumerNo) {
    	if(!this.fileReadCacheMap.containsKey(consumerNo)) {
    		System.out.println("Error! doesnot have consumer:" + consumerNo);
    		return true;
    	}
    	DefaultBytesMessage msg = this.fileReadCacheMap.get(consumerNo).readMessage();
    	if(msg == null) {
    		// 说明当前这个消费者已经把自己的消息文件消费完了
    		this.latch.countDown();
    		return true;
    	}
    	String topicName = msg.headers().getString(MessageHeader.TOPIC);
    	String queueName = msg.headers().getString(MessageHeader.QUEUE);
    	String actualName = (topicName == null? queueName : topicName);
    	// 分发消息
    	notifyMsg(MyUtil.getTopicNo(actualName), msg);
    	return false;
    }
    
    // 返回某个topic是否有消费者订阅
    public boolean topicHasConsumer(Integer topicNo) {
    	if(!this.topicConsumerMap.containsKey(topicNo)) {
    		return false;
    	}
    	if(this.topicConsumerMap.get(topicNo).isEmpty()) {
    		return false;
    	}
    	return true;
    }
    // 将topic为topicNo的消息分发出去
    public void notifyMsg(final Integer topicNo, final DefaultBytesMessage msg) {
    	if(!this.topicConsumerMap.containsKey(topicNo)) {
    		return;
    	}
    	for(final DefaultPullConsumer consumer : this.topicConsumerMap.get(topicNo)) {
    		consumer.notifyMsg(msg);
    	}
    }
    
    /**
     * Consumer将自己注册到MessageLoad，
     * 以便MessageLoad统计对于每个topic来说，有多少consumer订阅了它
     * @param consumer 消费者
     * @param topics 
     */
    public void registerConsumer(final DefaultPullConsumer consumer,
    			final Set<Integer> topicNO) {
    	this.consumerSet.add(consumer);
    	
    	// TODO:假设是线程安全的
    	for(final Integer tno : topicNO) {
    		if(!this.topicConsumerMap.containsKey(tno)) {
    			this.topicConsumerMap.put(tno, new ArrayList<DefaultPullConsumer>());
    		}
    		this.topicConsumerMap.get(tno).add(consumer);
    	}
    }
}