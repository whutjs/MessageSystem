package io.openmessaging.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



/**
 * 对应于每一个topic或者queue文件，用来保存写到文件的消息的缓存。
 * @author Jenson
 *
 */
public final class FileWriteCache {
	// 用来获取编号
	private static AtomicInteger globalNo = new AtomicInteger(0);	
	/** 5/27架构大改：每个Producer独享一个FileWriteCache */
	// 文件对应的文件名和路径
	private final String filePath;
	// 表示cache已经写到哪个位置了（其实就是长度）
	private int pos;	
	
	/* 5/25 更新：用map file写文件 */
	private RandomAccessFile raf;
	private FileChannel outChannel;
//	private MappedByteBuffer mappedFile;
	
	// 已经写入文件的字节数
	private long bytesWrite = 0;
	
	/* 不压缩的话要少用内存 */
	private final static int SIZE = (15*1024*1024);
	
	/** 线上测试 */
	private final static int FILE_SIZE = (172 * 1024 *1024);
	
	/** 本地测试 */
//	private final static int FILE_SIZE = (3 * 1024 *1024);
	
	/** 6/1 大更新：用一个87MB的DirectBuffer先缓存完全部数据，等到写完之后再一次性刷到磁盘！
	 * 避免pagecache每5s就 同步一次！
	 */
	private ByteBuffer wholeData;
	// 一次写87MB，足够大了，应该加锁避免同时多个线程写文件
	private static final Lock diskLock = new ReentrantLock();
	
	/**
	 * @param name topic或者queue 
	 * @throws IOException 
	 */
	public FileWriteCache(final String name) throws IOException {
		this.wholeData = ByteBuffer.allocateDirect(SIZE);
		filePath = name;
		pos = 0;
		File file = new File(filePath);
		if(file.exists()) {
			file.delete();
		}
		file.createNewFile();
		this.raf = new RandomAccessFile(file, "rw");
		this.outChannel = this.raf.getChannel();
	}
	
	/**
	 * 直接将消息写到MappedByteBuffer
	 * @param data
	 */
	public void write(final int topicNo, final byte[] data, final int dataLen) {
		// 5/26 15:30 更新：FileWriteCache只负责写入数据到map file
		writeData(topicNo, data, dataLen);		
	}
	
	private void flush2File() {
		this.wholeData.flip();
		if(this.wholeData.hasRemaining()) {
			try {
				diskLock.lock();
				while(this.wholeData.hasRemaining()) {
					this.outChannel.write(wholeData);
				}		
			} catch (IOException e) {
				e.printStackTrace();
			}finally {
				diskLock.unlock();
			}
		}
		this.wholeData.clear();
	}
	
	private void writeData(final int topicNo, final byte[] data, final int len) {
		if(len + 7 > this.wholeData.remaining()) {
			// 写满了，flush
			flush2File();
		}
		// 5/27更新：加上topicNo
		this.wholeData.put((byte)topicNo);
		// 还有chunk的长度类型
		if(len <= Byte.MAX_VALUE) {
			this.wholeData.put(Constant.MSG_LEN_BYTE);
			this.wholeData.put((byte)len);
		}else if(len <= Short.MAX_VALUE) {
			this.wholeData.put(Constant.MSG_LEN_SHORT);
			this.wholeData.putShort((short)len);
		}else {
			this.wholeData.put(Constant.MSG_LEN_INT);
			this.wholeData.putInt(len);
		}
		this.wholeData.put(data, 0, len);
	}
	/**
	 * 强制flush数据到硬盘。如果cache还有数据，先把数据写进文件。
	 */
	public void flush() {		
		flush2File();
		this.wholeData = null;
	}
	
	public void close() {
		try {
			this.outChannel.close();			
			this.raf.close();			
		} catch (IOException e) {
			MyLogger.log(MyLogger.ERROR_TAG, "Failed in FileWriteCache.close()."+e.getMessage());
			e.printStackTrace();
		}		
	}
	
}

