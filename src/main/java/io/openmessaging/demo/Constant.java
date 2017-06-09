package io.openmessaging.demo;

public class Constant {
	/** 6/2 改消息序列化方式：有些固定出现的字符串可以hardcode */
	public static final String TOPIC_KEY = "Topic";
	public static final String QUEUE_KEY = "Queue";
	public static final String MSGID_KEY = "MessageId";
	public static final String PRO_OFFSET_KEY = "PRO_OFFSET";
	public static final byte MSGID_KEY_BYTE = -2;
	public static final byte PRO_OFFSET_KEY_BYTE = -1;
	
	
	// 单条消息的最大大小256KB
	// 4/24 Update:官方说不会超过128 byte
	// 5/18 更新：又说最大可能有100KB
	/** 6/3更新：貌似最大不会超过32KB */
    public static final int MAX_MSG_SIZE = (32*1024);
    
    // 表示对topic queue分成多少个组
    public static final int GROUP_NUM = 10;
    
	/* 以下用于MSG体中区分各个部分 */
	// 表示接下来的MSG_LEN是byte类型
	public static final byte MSG_LEN_BYTE = 2;
	// 表示接下来的MSG_LEN是short类型
	public static final byte MSG_LEN_SHORT = 3;
	// 表示接下来的MSG_LEN是int类型
	public static final byte MSG_LEN_INT = 4;
	
	// 表示接下来的数据是消息头，msg_header
	public static final byte MSG_HEADER_BEGIN = 17;
	// 表示消息自定义的property部分的开始
	public static final byte MSG_PROPERTY_BEGIN = 5;
	// 表示消息body的开始
	public static final byte MSG_BODY_BEGIN = -3;
	
	// 表示接下来的BODY_LEN是byte类型
	public static final byte MSG_BODY_LEN_BYTE = 2;
	// 表示接下来的BODY_LEN是short类型
	public static final byte MSG_BODY_LEN_SHORT = 3;
	// 表示接下来的BODY_LEN是int类型
	public static final byte MSG_BODY_LEN_INT = 4;
	
	
	/* 以下用于KEY-VALUE编码*/

	// int类型
	public static final byte V_INT_TYPE = 2;
	// long类型
	public static final byte V_LONG_TYPE = 3;
	// double
	public static final byte V_DOUBLE_TYPE = 4;
	// String
	public static final byte V_STRING_TYPE = 23;
	// 表示一个KEY_VALUE的开始
	public static final byte KEY_VALUE_BEGIN = 24;
	// 表示一个KEY_VALUE的结束
	public static final byte KEY_VALUE_END = 14;
	
	/* 表示大小 */
	public static final int SIZE_512KB = (1<<19);
	public static final int SIZE_1MB = (1<<20);
	public static final int SIZE_2MB = (1<<21);
	public static final int SIZE_4MB = (1<<22);
	public static final int SIZE_8MB = (1<<23);
	public static final int SIZE_16MB = (1<<24);
	public static final int SIZE_18MB = (18*1024*1024);
	public static final int SIZE_20MB = (20*1024*1024);
	public static final int SIZE_50MB = (50*1024*1024);
	public static final int SIZE_200MB = (180*1024*1024);
	
	/* 存储数据的文件名 */
	public static final String STORE_FILE_PREFIX = "jenson_msg";
	
	public static final String TOPIC_NO_FILE = "metadata";
	
	/** 表示消息压缩的chunk的开始 */
	public static final byte COMP_CHUNK_BEGIN = 66;
	// chunk表示法:
	// COMP_CHUNK_BEGIN + chunk_len_type(MSG_LEN_BYTE|MSG_LEN_SHORT|MSG_LEN_INT)
	// + chunk_len (byte | short | int) + CHUNK
	// 也就是每个chunk至少有额外3byte
	
}
