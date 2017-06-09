package io.openmessaging.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// 5/27 增加。用于计算topic queue的hash值
public class MyUtil {
	// 保存topic字符串-> topic编号的映射
	private static ConcurrentHashMap<String, Integer> topicNo = new ConcurrentHashMap<>();
	// 保存编号到字符串的映射，在消费时需要
	private static ConcurrentHashMap<Integer, String> topicNo2Name = new ConcurrentHashMap<>();
	private static AtomicInteger globalTopicNo = new AtomicInteger(0);
	
	public static ConcurrentHashMap<String, Integer> getTopicNoMap() {
		return topicNo;
	}
	// 获得某个topic的编号
	public static Integer getTopicNo(final String name) {
		if(name == null || name.length() == 0) {
			return -1;
		}
		if(!topicNo.containsKey(name)) {
			synchronized (topicNo) {
				if(!topicNo.containsKey(name)) {
					int gno = globalTopicNo.getAndIncrement();	
					topicNo.put(name, gno);
				}
			}
		}
		return topicNo.get(name);
	}
	
	public static String getTopicName(final Integer tpno) {
		return topicNo2Name.get(tpno);
	}
	// 从文件中重新构造topic和no
	public static void constructMetaData(final String storePath) {
		File topicFile = Paths.get(storePath, Constant.TOPIC_NO_FILE).toFile();
		try(BufferedReader reader = new BufferedReader(new FileReader(topicFile))) {
			String line = null;
			while((line = reader.readLine()) != null) {
				int idx = line.indexOf(':');
				String name = line.substring(0, idx);
				String no = line.substring(idx+1);
				Integer tpn = Integer.parseInt(no);
				topicNo.put(name, tpn);
				topicNo2Name.put(tpn, name);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
