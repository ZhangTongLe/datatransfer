package com.iflytek.dts;

import com.iflytek.common.LogUtils;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public class DatabaseTransferThread implements Callable<Integer> {
    /**
     * log
     */
    Logger logger = LogUtils.getLogger(DatabaseTransferThread.class);
    /**
     * 原数据库数据库表名
     */
    private String sourceTable;

    /**
     * 目标数据库表名
     */
    private String destTable;

    /**
     *
     * @return
     * @throws Exception
     */

    public DatabaseTransfer databaseTransfer;

    public DatabaseTransferThread(DatabaseTransfer databaseTransfer,String sourceTable,String destTable){
        this.databaseTransfer = databaseTransfer;
        this.sourceTable = sourceTable;
        this.destTable = destTable;
    }
    @Override
    public Integer call() throws Exception {
        int result = 0;
        try {
            logger.info("[thread "+Thread.currentThread().getName()+"] start");
            databaseTransfer.getMoveDataCount(sourceTable);
            // tb_ism_dict_category tb_ism_knowledge_category tb_ism_template_category
            if("tb_ism_dict_category".equalsIgnoreCase(destTable)
                    || "tb_ism_knowledge_category".equalsIgnoreCase(destTable)
                    ||"tb_ism_template_category".equalsIgnoreCase(destTable)){
                return databaseTransfer.moveTablesDataWithTree(sourceTable,destTable);
            }
            result = databaseTransfer.moveTablesData(sourceTable,destTable);
        }catch (Exception e){
            logger.info("[thread "+Thread.currentThread().getName()+"] error",e);
            return -1;
        }

        return result;
    }
}
