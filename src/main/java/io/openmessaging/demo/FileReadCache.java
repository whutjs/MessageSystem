package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Inflater;


//用于给消息编号
class TopicMsgNO {
	int no = 0;
}

/**
 * 每个consumer都有一个FileReadCache,
 * 每次Consumer对某个topic进行poll消息，都先通过FileReadCache来读消息；
 * 如果FileReadCache缓存有消息，则直接返回，否则从硬盘读一块出来
 * 某个topic读完后，重新设置FileReadCache打开新的topic文件即可
 * 5/28 Update：采用push架构
 * @author Jenson
 *
 */
public final class FileReadCache {
	// 使用数字来替换生产者消息编号
	private HashMap<String, TopicMsgNO> msgNOMap;
	    
	/** 6/2更新：优化序列化反序列化 */
	private String curTopicOrQueName = null;
	
	/** 消费者编号 */
	private final int consumerNO;
	// 映射到的测评编号
	private final int testerNO;
	private final String PRO_OFFSET_PREFIX;
	
	//4MB
	private final static long SIZE = Constant.SIZE_18MB;
	// 使用Memory mapped file试试
	private MappedByteBuffer buf = null; 
	private RandomAccessFile raf;
	private FileChannel	inChannel;
	
	// 文件名，用来打log
	private final String fileName;
	// 在extractKeyValue里用来重复利用保存key的字节数组
	byte[] keyBytes;
	// 在extractKeyValue里用来重复利用保存val的字节数组
	byte[] valStrBytes;
	
	private byte[] chunk;
	// 用来wrap 解压后的chunk的ByteBuffer
	private ByteBuffer msgChunkBuf;
	// 要解压缩的chunk大小，最多1MB
	private final static int CHUNK_SIZE = Constant.SIZE_1MB;
	
	public FileReadCache(final int csno, final String fileName) {
		this.msgNOMap = new HashMap<>(100);
		 
		this.consumerNO = csno;
		this.testerNO = this.consumerNO;
		this.PRO_OFFSET_PREFIX = "PRODUCER"+this.testerNO+"_";
				
		this.fileName = fileName;
		this.keyBytes = new byte[128];
		this.valStrBytes = new byte[Constant.MAX_MSG_SIZE];
		
		/** 5/26新增：解压缩 */
		this.chunk = new byte[CHUNK_SIZE];
		this.msgChunkBuf = null;
		
		openFile();
	}
	
	private void openFile() {
		try {			
			this.raf = new RandomAccessFile(this.fileName, "r");
			this.inChannel = raf.getChannel();
		} catch (FileNotFoundException e) {
			String errMsg = "Failed in openTopicFile. File "+ fileName+" doesnot exist"
					+ e.getMessage();
			MyLogger.log(MyLogger.ERROR_TAG, errMsg);
			throw new ClientOMSException(errMsg);
		}
		// 5/23更新：用mmap file读文件
		try {
			this.buf = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error mmap file:" + e.getMessage());
		}
	}
	
	
	// 5/25更新：由于是一次性读入，所以不用考虑消息长度标识，也不用考虑数据没有读完到cache
	// 从buf中读取并解析出一条消息
	private DefaultBytesMessage decodeMsg() {
		// 如果chunk没有数据了，从mapFileBuffer里面读出来
		if(this.msgChunkBuf == null || 
				this.msgChunkBuf.hasRemaining() == false) {
			while(true) {
				// CHUNK至少有3字节
				int leastBytesRemain = 3;		
				if(this.buf.remaining() <= leastBytesRemain) {
					// 不够就说明读完了
					return null;		
				}
				// 第一位应该是topicNo的byte
				int topicNo = this.buf.get();
				// 看看这个topic有没有消费者订阅
				boolean hasConsumer = MessageLoad.getInstance().topicHasConsumer(topicNo);				
				byte chunkLenType = this.buf.get();
				int chunkLen = 0;
				switch(chunkLenType) {
					case Constant.MSG_LEN_BYTE:
						chunkLen = this.buf.get();
						break;
					case Constant.MSG_LEN_SHORT:
						chunkLen = this.buf.getShort();
						break;
					case Constant.MSG_LEN_INT:
						chunkLen = this.buf.getInt();
						break;
					default:
						MyLogger.log(MyLogger.ERROR_TAG, "unknown chunkLenType! chunkLenType="+chunkLenType);
						return null;
				}
				if(!hasConsumer) {
					// 如果当前topic没人消费,skip
					this.buf.position(this.buf.position() + chunkLen);
					continue;
				}
				this.curTopicOrQueName = MyUtil.getTopicName(topicNo);
				// 将chunk数据读进来
				this.buf.get(this.chunk, 0, chunkLen);
				/** 6/3 更新：不压缩 */
				// wrap
				this.msgChunkBuf = ByteBuffer.wrap(this.chunk);
				this.msgChunkBuf.limit(chunkLen);
				
				if(hasConsumer) {
					break;
				}
			}
		}					
		DefaultBytesMessage msg = extractMessage(this.msgChunkBuf);
		return msg;
	}
	
	/**
	 * 提取出头部
	 * @param msgBuf
	 * @param headers
	 */
	private void extractHeaders(final ByteBuffer msgBuf, DefaultKeyValue headers) {
		// 第一个是MSG_ID_LEN
		int len = msgBuf.getShort();	
		msgBuf.get(valStrBytes, 0, len);
		headers.put(Constant.MSGID_KEY, new String(valStrBytes, 0, len));
		// 接下来放入头部的Topic字段
		String key = Constant.TOPIC_KEY;
		if(this.curTopicOrQueName.charAt(0) == 'Q') {
			key = Constant.QUEUE_KEY;
		}
		headers.put(key, this.curTopicOrQueName);
	}
	
	/**
	 * 提取出消息。
	 * @param msgBuf
	 * @return
	 */
	private DefaultBytesMessage extractMessage(final ByteBuffer msgBuf) {
		DefaultBytesMessage msg = new DefaultBytesMessage(null);
		//第一byte是MSG_HEADER_BEGIN
		if(msgBuf.get() != Constant.MSG_HEADER_BEGIN) {
			String errMsg = "Failed to extractMessage in FileReadCache.extractMessage() MSG_HEADER_BEGIN expected.";
			MyLogger.log(MyLogger.ERROR_TAG, errMsg);
			//throw new ClientOMSException(errMsg);
			return null;
		}
		DefaultKeyValue headers = new DefaultKeyValue();
		extractHeaders(msgBuf, headers);
		msg.setHeader(headers);
		DefaultKeyValue property = new DefaultKeyValue();
		byte nxtByte = extractKeyValue(msgBuf, property);
		msg.setProperty(property);		
		if(nxtByte != Constant.MSG_BODY_BEGIN) {
			// 如果不等于MSG_BODY_BEGIN，报错
			String errMsg = "Failed to extractMessage in FileReadCache. MSG_BODY_BEGIN expected.";
			MyLogger.log(MyLogger.ERROR_TAG, errMsg);
			//throw new ClientOMSException(errMsg);
			return null;
		}
		int bodyLen = msgBuf.getShort();
		byte[] body = new byte[bodyLen];
		msgBuf.get(body);
		msg.setBody(body);
		return msg;
	}
	
	/**
	 * 提取出keyvalue，将结果存到dstKV中。
	 * 返回最后一个读到的byte
	 */
	private byte extractKeyValue(ByteBuffer msgBuf, 
					DefaultKeyValue dstKV) 
	{
		/**
    	 * 6/3 更新：header部分只存messageId的value，key都不用存；Topic也不用存！
    	 * Property的key：PRO_OFFSET 用1byte表示！
    	 * 一条消息编码为：
    	 * 头部：MSG_HEADER_BEGIN + MSG_ID_LEN(Short)+ MSG_ID
    	 * Property: (KEY_LEN(byte) + key_str + VALUE_LEN (Short) + VALUE_STR)
    	 * 			 ....
    	 * MSG_BODY: MSG_BODY_BEGIN (-3) + MSG_BODY_LEN (Short) + MSG_BODY
    	 */
		// 先放PRO_OFFSET		
		if(!this.msgNOMap.containsKey(this.curTopicOrQueName)) {
			this.msgNOMap.put(curTopicOrQueName, new TopicMsgNO());
		}
		int msgNO = (this.msgNOMap.get(curTopicOrQueName).no)++;
		StringBuilder sb = new StringBuilder();
		sb.append(PRO_OFFSET_PREFIX).append(msgNO);
		dstKV.put(Constant.PRO_OFFSET_KEY, sb.toString());
		
		// 一个keyvalue字段，如果非空，那么除去KEY_VALUE_BEGIN，至少
		// 还需要有KEY_LEN 和 VALUE_TYPE两字节
		while(msgBuf.hasRemaining()) {			
			byte keyLen = msgBuf.get();
			if(keyLen == Constant.MSG_BODY_BEGIN) {
				return keyLen;
			}
			String key = null;
			msgBuf.get(keyBytes, 0, keyLen);
			// NOTE: 底层实现会根据byte[]内容新构建一个char[]，也就是构造函数传进来的byte[]内容相当于被复制了！
			// 所以为了避免每次都新建一个byte[]，可以构造一个全局的byte[]，重复利用
			key = new String(keyBytes, 0, keyLen);
			int valLen = msgBuf.getShort();
			msgBuf.get(valStrBytes, 0, valLen);					
			dstKV.put(key, new String(valStrBytes, 0, valLen));			
		}
		return Constant.MSG_BODY_BEGIN;
	}
	
	// 目前是一个Consumer一个，也就是一个线程一个，不用锁
	// 2017/4/19
	/**
	 * 从文件读取数据，并解析消息。出错或者读完都返回null;
	 * @return
	 */
	public DefaultBytesMessage readMessage() {
		return decodeMsg();	
	}
	
}
