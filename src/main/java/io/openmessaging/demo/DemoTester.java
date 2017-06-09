package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

import java.awt.SystemColor;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;

public class DemoTester {
	static byte[] msgBody = new byte[64];
	static void fillMsgBody() {
		for(int i = 0; i < 64; ++i)
			msgBody[i] = (byte) i;
	}
	// 保存topic
	static ArrayList<String> topics = new ArrayList<>(100);
	// 保存queue名字
	static ArrayList<String> queues = new ArrayList<>(100);
	static String[] topicSeed = {
		"Alibaba", "Tencent", "Baidu", "Microsoft", "Huawei", "Google",
		"Intel", "Amazon", "NetEase", "Facebook", "DiDi"
	};
//	private static String storePath = "/home/sfc/jenson/alibaba/2017/store";
	private static String storePath = "E:\\Contest\\阿里中间件2017\\store";
	// 从文件中读出topic和queue的名字
	private static void constrcutTopicAndQueue() throws FileNotFoundException, IOException {
		File topicFile = Paths.get(storePath, Constant.TOPIC_NO_FILE).toFile();
		try(BufferedReader reader = new BufferedReader(new FileReader(topicFile))) {
			String line = null;
			while((line = reader.readLine()) != null) {
				int idx = line.indexOf(':');
				String name = line.substring(0, idx);
				String no = line.substring(idx+1);
				if(name.charAt(0) == 'Q') {
					// 是队列
					queues.add(name);
				}else{
					topics.add(name);
				}
//				System.out.println("name:"+name+" no:"+no);
			}
		}
//		System.out.println("Queues:");
//		for(final String que : queues) {
//			System.out.println(que);
//		}
//		System.out.println("Topics:");
//		for(final String topic : topics) {
//			System.out.println(topic);
//		}
		
	}
	// 构造Num条topic并保存到topics
	private static void constructNumTopics(final int num) {
		Random rand = new Random();
		for(int i = 0; i < num; ++i) {
			int idx = rand.nextInt(topicSeed.length);
			topics.add(topicSeed[idx] + i);
		}
	}
	// 构造Num条queue并保存到queues
	private static void constructNumQueues(final int num) {
		Random rand = new Random();
		String queName = "QUEUE";
		for(int i = 0; i < num; ++i) {
			queues.add(queName + i);
		}
	}
	
	final static String strVal = "HelloWorld";
	private static Message constructMsg(final String topic, final Producer producer) {
		Message msg = null;
//	    Random rand = new Random();
    	if(topic.charAt(0) != 'Q') {
    		msg = producer.createBytesMessageToTopic(topic,  msgBody);
    	}else{
    		msg = producer.createBytesMessageToQueue(topic, msgBody);
    	}
		String strKey = "strKey";		
    	// string
    	msg.putProperties(strKey, strVal);
       
    	return msg;
	}
	static volatile boolean t1TimeIsUp = false;
    public static void main(String[] args) {
    	MyLogger.log(MyLogger.INFO_TAG, "Start test..");
//		if(args.length < 1) {
//			System.out.println("Error. Too few arguments");
//			return;
//		}
//		// 是否是生产消息模式
//		final boolean isWriteMode = (args[0].equals("w")?true:false);
    	final boolean isWriteMode = false;
    	// topic和队列总共的数目
    	final int totalTopicAndQueueCnt = 100;
    	// 队列的数目,应该>=消费者数目
    	final int queueCnt = 10;
    	// 线程数，应该<=queueCnt;
    	final int threadCnt = queueCnt;
    	final int MSG_NUM = 40000;
    	
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
    	KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        properties.put("STORE_PATH", storePath);
        AtomicLong time = new AtomicLong(0);
        ExecutorService consumerThreads = Executors.newFixedThreadPool(threadCnt);
        // 给线程编号用
        final AtomicInteger globalThreadNo = new AtomicInteger(0);        
        
    	if(isWriteMode) {
    		fillMsgBody();
    		RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName();
            System.out.println("当前进程的标识为："+name);
	    	constructNumTopics(totalTopicAndQueueCnt-queueCnt);
	    	constructNumQueues(queueCnt);
	        
    		try {
				Thread.sleep(3000);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
	        
	        // 保存所有的topic和queue
	        ArrayList<String> totalTopicAndQueue = new ArrayList<>();
	        totalTopicAndQueue.addAll(topics);
	        totalTopicAndQueue.addAll(queues);
	        final int totalTopicQueSize = totalTopicAndQueue.size();
	        ExecutorService producerThreads = Executors.newFixedThreadPool(threadCnt);
	        	       
	        // 总共已经发送的消息
	        AtomicLong totalSentMsg = new AtomicLong(0);       
			
			System.out.println("Start sending msg...");
			
			long startTime = System.currentTimeMillis();
			
	        for(int cnt = 0; cnt < threadCnt; ++cnt) {
	        	producerThreads.submit(()->{
						// 每个生产者生产400万
		        		final long sendMsgPerProducer = MSG_NUM;
		        		// 已经发送的消息
						long msgSent = 0;
			        	int threadNo = globalThreadNo.getAndIncrement();
			        	// 每个线程（生产者）随机生产numMsgPerThread条消息
			        	Producer threadP = new DefaultProducer(properties);
			        	Random rand = new Random();
			        	while(msgSent < sendMsgPerProducer) {
			        		// 随机取一个topic
			        		int idx = rand.nextInt(totalTopicQueSize);
			        		String topic = totalTopicAndQueue.get(idx);
			        		Message msg = constructMsg(topic, threadP);
			        		threadP.send(msg);
			        		msgSent++;
			        	}
			        	threadP.flush();
			        	totalSentMsg.addAndGet(msgSent);
					}	        					   
	        	);	  
	        }
	        try {
				producerThreads.shutdown();
				// 直接模拟等5分钟
		        producerThreads.awaitTermination(5, TimeUnit.MINUTES);
		        t1TimeIsUp = true;
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
	        long endTime = System.currentTimeMillis();
	        double timeSec = (endTime - startTime) / 1000.0;
	        long totalMsg = totalSentMsg.get();
	        double sentTps = (totalMsg/timeSec);
	        System.out.println("Sending " + totalMsg + 
	        		" takes:" + timeSec + " S Send TPS:" + sentTps);
	        
    	}else {
    		try {
				constrcutTopicAndQueue();
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
	        globalThreadNo.set(0);
	        time.set(0);
	        final AtomicLong actualConsumeMsgNum = new AtomicLong(0);
			System.out.println("Total topics:"+ topics.size()+" queue:"+queues.size());
			System.out.println("Start consuming msg...");
			long startConsumer = System.currentTimeMillis();
			// 顺序构造threadCnt个消费者
			final ArrayList<DefaultPullConsumer> csList = new ArrayList<>(threadCnt);
			final HashMap<Integer, Set<String>> csTopicSet = new HashMap<>();
			final HashMap<Integer, String> csQueueMap = new HashMap<>();
			for(int i = 0; i < threadCnt; ++i) {
				DefaultPullConsumer consumer = new DefaultPullConsumer(properties);
				Random rand = new Random();
	        	int totalTopicCnt = topics.size();
	        	// 每个消费者订阅的topic数
	        	int nTopics = totalTopicCnt/queueCnt;		        	
	        	
	        	// 绑定的queue
	        	final String queue = queues.get(i);
	        	csQueueMap.put(i, queue);
	        	// 随机订阅topic 
	        	Set<String> subscribeTopics = new HashSet<String>(); 	        	
	        	while(subscribeTopics.size() < nTopics) {
	        		int topicIdx = rand.nextInt(totalTopicCnt);
	        		String topic = topics.get(topicIdx);
	        		if(subscribeTopics.contains(topic)) {
	        			continue;
	        		}
	        		subscribeTopics.add(topic);
//	        		topicOffset.put(topic, 0);
	        	}
	        	StringBuilder sb = new StringBuilder();
	        	sb.append("Consumer ").append(i).append(" subscribe ")
	        		.append(subscribeTopics.size()).append(" topics:\n");
	        	for(String t : subscribeTopics) {
	        		sb.append(t).append("\n");
	        	}
	        	MyLogger.log(MyLogger.INFO_TAG, sb.toString());
	        	consumer.attachQueue(queue, subscribeTopics);
	        	csList.add(consumer);
	        	csTopicSet.put(i, subscribeTopics);
			}
	        for(int i = 0; i < threadCnt; ++i) {
		        // 同样的，每个线程起一个消费者，每个消费者绑定一个queue
	        	consumerThreads.submit(()-> {			
	        			int threadNo = globalThreadNo.getAndIncrement();
	        			DefaultPullConsumer consumer = csList.get(threadNo);
	        			Set<String> subscribeTopics = csTopicSet.get(threadNo);
	        			final String queue = csQueueMap.get(threadNo);
			        	// 接下来开始消费
			        	MyLogger.log(MyLogger.INFO_TAG, "Consumer "+threadNo+
				        			" start polling msg.");	
			        	// 休眠一下模拟实际测评时是单线程构造
			        	try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
			        	// 实际消费的
			        	long consumeMsg = 0;
			        	// 每个消费者最多消费400万
		        		final long maxConsumedMsg = MSG_NUM;
			        	while (true) {
			        		// 每个消费者最多消费
			        		if(consumeMsg >= maxConsumedMsg) {
			        			break;
			        		}
							// 至少消费了一条消息
							boolean consumeMsgSucceed = false;
							try{
			                	Message message = consumer.poll();
			                	if (message == null) {
			                    	//拉取为null则认为消息已经拉取完毕
			                    	break;
			                	}
//			                	MyLogger.log(MyLogger.INFO_TAG, "Consumer:"+threadNo+" "+message.toString());
			                	++consumeMsg;
								/* TODO:为了测试吞吐量，不用比较了 */
			                	String topic = message.headers().getString(MessageHeader.TOPIC);
			                	String queueName = message.headers().getString(MessageHeader.QUEUE);
			                	String actualName = null;
				                //实际测试时，会一一比较各个字段
				                if (topic != null) {
			    	                if(!subscribeTopics.contains(topic)) {
			        	            	String errMsg = "Consumer(Thread) "+threadNo
			            	        			+" doesnot have topic:"+topic;
			                	    	MyLogger.log(MyLogger.ERROR_TAG, errMsg);
			                    		System.out.println(errMsg);
			                    		throw new ClientOMSException(errMsg);
			                   	 	}
			                    	actualName = topic;
				                } else if(queueName != null){
				                	if(!queueName.equals(queue)) {
			    	            		String errMsg = "Consumer(Thread) "+threadNo
			        	            			+" attach to queue:"+queue
			            	        			+" but consume msg from queue:"+queueName;
			                	    	MyLogger.log(MyLogger.ERROR_TAG, errMsg);
			                    		System.out.println(errMsg);
			                    		throw new ClientOMSException(errMsg);
			                		}
				                    actualName = queueName;
				                }else{
			    	            	String errMsg = "Consumer(Thread) "+threadNo
			        	            			+" both topic and queueName are null!";
			                	    MyLogger.log(MyLogger.ERROR_TAG, errMsg);
			                    	System.out.println(errMsg);
			                    	throw new ClientOMSException(errMsg);
								}
								consumeMsgSucceed = true;
							}catch(Exception ex) {
								String errMsg = "Error in consumer:"+threadNo+" ever consumer a msg?:"+consumeMsgSucceed+" err msg:"+ex.getMessage();
								MyLogger.log(MyLogger.ERROR_TAG, errMsg);
								ex.printStackTrace();
			                    throw new ClientOMSException(errMsg);
							}
			            }
			        	
			        	actualConsumeMsgNum.addAndGet(consumeMsg);
						MyLogger.log(MyLogger.INFO_TAG, "Consumer "+threadNo+" finish polling msg.");
						
					}
				);
	        }	       
	        consumerThreads.shutdown();
	        try {
	        	consumerThreads.awaitTermination(30, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        long endConsumer = System.currentTimeMillis();             
        	double T2 = (endConsumer - startConsumer)/1000.0;
	        double tps = (actualConsumeMsgNum.get()/T2);
	        System.out.println("Actual consume msg:" + actualConsumeMsgNum.get());
	        System.out.println("T2="+(T2) + "S\n"	        			
	        			+ "Consume Tps:" + tps);
        
    	}

    }
}

