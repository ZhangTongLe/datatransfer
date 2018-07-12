package com.iflytek;

import com.iflytek.common.CmdLine;
import com.iflytek.common.Constants;
import com.iflytek.common.GlobalCommon;
import com.iflytek.common.LogUtils;
import com.iflytek.dts.DatabaseTransfer;
import com.iflytek.dts.DatabaseTransferThread;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @desc: 工具入口
 * @author: jyli
 */
public class BootStrap {

    /**
     * 日志记录文件
     */
    private static Logger logger = LogUtils.getLogger(BootStrap.class);

    /**
     * 入口函数
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        CmdLine cl = CmdLine.parse(args);
        if (cl == null || !cl.validateCmdLine()) {
            logger.error("命令行输入参数错误");
            return;
        }
        Properties properties = GlobalCommon.loadConfig(cl.getPath());
        int thread_num = Integer.valueOf(properties.getProperty(Constants.THREAD_NUM, "5"));
        DatabaseTransfer db = new DatabaseTransfer(properties);
        logger.info("创建线程池...");
        ExecutorService transferSvc = Executors.newFixedThreadPool(thread_num);
        List<String> sources = db.getSourceTables();
        List<String> dests = db.getDestTables();
        Map<String, String> linked = db.linkTables(sources, dests);
        if (cl.isPreClear()) {
            for (Map.Entry<String, String> entry : linked.entrySet()) {
                db.preClearDestTableContent(entry.getValue());
            }
        }
        Map<String, Future<Integer>> futures = new HashMap<>();
        String prefix = null;
        if ("PERMISSION".equalsIgnoreCase(cl.getType())) {
            prefix = "TB_PERMISSION";
        } else if ("ISM".equalsIgnoreCase(cl.getType())) {
            prefix = "TB_ISM";
        }
        for (Map.Entry<String, String> entry : linked.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                //if (entry.getValue().equalsIgnoreCase("TB_PERMISSION_USER")){
                futures.put(entry.getKey(), transferSvc.submit(new DatabaseTransferThread(db, entry.getKey(), entry.getValue())));
            }
        }
        for (Map.Entry<String, Future<Integer>> entry : futures.entrySet()) {
            Integer ret = futures.get(entry.getKey()).get();
            if (ret == -1) {
                logger.error(String.format(Locale.CHINA, "table " + entry.getKey() +
                        " 迁移失败,请查看log"));
            } else {
                logger.info(String.format(Locale.CHINA, "table " + entry.getKey() +
                        " 执行完毕,转移数据" + ret + "条"));
            }

        }
        Thread.sleep(1000L);
        logger.info("关闭连接池");
        db.close();
        logger.info("关闭线程池中...");
        transferSvc.shutdown();
        if (transferSvc.isTerminated()) {
            logger.info("线程池已关闭");
        }
        logger.info("数据传输完成");

    }

}
