package com.iflytek.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.*;

/**
 * @desc: 日志文件
 * @author: yhsu
 * @createTime: 2017年3月7日 下午2:34:48
 * @version: 2.0
 */
public class LogUtils {
    // 制定加载配置文件
    static {
        FileInputStream fis = null;
        try {

            File configFile = new File( Constants.LOG4J2_XML);
//            File configFile = new File("F:\\Workspace\\SE_EVE3.7\\Branches\\ChinaMobileOnlineService\\datatransfer\\src\\main\\resources\\log4j2.xml");
            fis = new FileInputStream(configFile);
            ConfigurationSource source = new ConfigurationSource(
                    fis, configFile.toURL());
            Configurator.initialize(null, source);
        } catch (Exception e) {
            LogManager.getLogger().error("读取Log4j2配置文件时出错", e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    LogManager.getLogger().error("关闭Log4j2配置文件时出错", e);
                }
            }
        }
    }

    public static Logger getLogger() {
        return LogManager.getLogger();
    }

    /**
     * @param name
     * @return Logger
     * @author: yhsu
     * @createTime: 2017年3月7日 下午2:41:47
     */
    public static Logger getLogger(String name) {
        return LogManager.getLogger(name);
    }

    /**
     * @param clazz
     * @return Logger
     * @author: yhsu
     * @createTime: 2017年3月7日 下午2:41:45
     */
    public static Logger getLogger(Class<?> clazz) {
        return LogManager.getLogger(clazz);
    }

    public static String Exception2Msg(Exception e) {
        StringWriter sw = null;
        PrintWriter pw = null;
        try {
            sw = new StringWriter();
            pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "\r\n" + sw.toString() + "\r\n";
        } catch (Exception e2) {
            return "bad getErrorInfoFromException";
        } finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
            if (pw != null) {
                pw.close();
            }
        }
    }

}
