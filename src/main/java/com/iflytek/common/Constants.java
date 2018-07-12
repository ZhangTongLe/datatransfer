package com.iflytek.common;


/**
 * @desc: 常量类
 * @author: JYLI
 * @version: 2.0
 */
public class Constants {

	/**
	 * 路径分隔符
	 */
	public static final String PROPERTIES = System.getProperty("file.separator");

	/**
	 * 当前路径
	 */
	public static final String USER_DIR = System.getProperty("user.dir");
	/**
	 * 日志配置路径
	 */
	public static final String LOG4J2_XML =   "log4j2.xml";

	public static final String THREAD_NUM = "thread.num";
	public interface SourceDB{
		public static final String DRIVER = "source.driver";
		public static final String URL = "source.url";
		public static final String USERNAME = "source.username";
		public static final String PASSWD = "source.password";

	}
	public interface DestDB{
		public static final String DRIVER = "dest.driver";
		public static final String URL = "dest.url";
		public static final String USERNAME = "dest.username";
		public static final String PASSWD = "dest.password";
	}
}
