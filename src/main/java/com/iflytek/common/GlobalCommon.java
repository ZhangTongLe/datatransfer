package com.iflytek.common;

import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @desc: 配置文件读取
 * @author: yhsu
 * @createTime: 2017年3月13日 上午10:30:22
 * @version: 2.0
 */
public class GlobalCommon {

    private static Logger logger = LogUtils.getLogger(GlobalCommon.class);

    /**
     * 加载ES配置文件
     *
     * @return Properties
     * @author: yhsu
     * @createTime: 2017年3月13日 上午10:30:54
     */
    public static Properties loadConfig(String path) {
        Properties peoperties = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(path));
            peoperties.load(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.error("未找到指定的配置文件，path：" + path);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("加载指定的配置文件出错，path：" + path);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    logger.error("关闭文件流出错，path：" + path);
                }
            }
        }
        return peoperties;
    }

    /**
     * 获取根目录
     *
     * @return
     */
    public static String getRootPath() {
        String rootPath = GlobalCommon.class.getClass().getProtectionDomain().getCodeSource()
                .getLocation().getPath();
        File file = new File(rootPath);
        rootPath = file.getParent();
        rootPath = rootPath.replaceAll("\\\\", "/");
        return rootPath;
    }
}
