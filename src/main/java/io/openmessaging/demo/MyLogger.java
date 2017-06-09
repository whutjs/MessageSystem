package io.openmessaging.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class MyLogger {
	public static final String ERROR_TAG = "ERROR";
	public static final String INFO_TAG = "INFO";
	
	public static final boolean ENABLE = false;
	public static final boolean LOCAL_RUN = false;
	private static final String fileName = "E:\\Contest\\阿里中间件2017\\log.txt";
//	private static final String fileName = "/home/sfc/jenson/alibaba/2017/log.txt";
	
	static{
		if(ENABLE && LOCAL_RUN) {
			File file = new File(fileName);
			if(file.exists()) {
				file.delete();
			}
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static synchronized void log(final String tag, final String msg) {
		if(!ENABLE) return;
		if(!LOCAL_RUN) {
		    System.out.println(tag+" :"+msg);
		    return;
		}
		try(BufferedWriter writer = 
				Files.newBufferedWriter(Paths.get(fileName), 
						StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
			writer.write(tag+" : " + msg);
			writer.newLine();
			//writer.write(tag + msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void listAllFiles(final String storePath) {
		double totalSizeInMB = 0;
		final double div = 1024*1024;
		File dir = new File(storePath);		
		for(final File file : dir.listFiles()) {
			double size = file.length() / div;
			System.out.println("File:"+file.getName()+" Size:" + size);
			totalSizeInMB += size;
		}
		System.out.println("totalSizeInMB:" + totalSizeInMB);
		
		
	}
}
