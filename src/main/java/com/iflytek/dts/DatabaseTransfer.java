package com.iflytek.dts;

import com.iflytek.common.*;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import oracle.sql.CLOB;
import oracle.sql.TIMESTAMP;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * 数据库oracle to mysql 转移服务
 * 当前应用于 知识库业务
 */
public class DatabaseTransfer {
    private static final Logger logger = LogUtils.getLogger(DatabaseTransfer.class);
    /**
     * 源数据库ds
     */
    private ComboPooledDataSource sourceDS;
    /**
     * 目标数据库ds
     */
    private ComboPooledDataSource destDS;

    /**
     * 初始化数据传输服务
     *
     * @param properties
     * @throws Exception
     */
    public DatabaseTransfer(Properties properties) throws Exception {
        sourceDS = DBUtils.createSourceDatasource(properties);
        destDS = DBUtils.createDestDatasource(properties);


    }

    /**
     * 列类型判断
     *
     * @param columnType
     * @return
     */
    private static boolean isNeedQuotation(String columnType) {
        columnType = columnType.toUpperCase();
        switch (columnType) {
            case "VARCHAR":
            case "VARCHAR2":
            case "CHAR":
                return true;
            case "NUMBER":
            case "DECIMAL":
            case "INT":
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
                return false;
            case "DATETIME":
            case "TIMESTAMP":
            case "DATE":
                return true;
            default:
                return true;
        }
    }

    /**
     * 预清空目标数据库表内容
     *
     * @param tableName
     */
    public void preClearDestTableContent(String tableName) {
        logger.info("清空数据库表:" + tableName);
        SQLExecutor.newInstance(destDS).clearTable(tableName);
    }

    /**
     * 源数据库表名称列表
     *
     * @return
     */
    public List<String> getSourceTables() {
        List<String> list = SQLExecutor.newInstance(sourceDS).showTables(sourceDS.getUser().toUpperCase());
        if (list != null) {
            list.removeIf(x -> !(x.startsWith("TB_ISM") || x.startsWith("TB_PERMISSION")));
        }
        logger.info("原数据库表列表:" + list);
        return list;
    }

    /**
     * 目标数据库表名称列表
     *
     * @return
     */
    public List<String> getDestTables() {
        List<String> list = SQLExecutor.newInstance(destDS).showTables(null);
        if (list != null) {
            list.removeIf(x -> !(x.startsWith("TB_ISM") || x.startsWith("TB_PERMISSION")));
        }
        logger.info("目标数据库表列表:" + list);
        return list;
    }

    /**
     * 源数据库名列表与表列表的对应映射
     *
     * @param source
     * @param dest
     * @return
     */
    public Map<String, String> linkTables(List<String> source, List<String> dest) {
        logger.info("原数据库 表列表：" + source.toString());
        logger.info("目标数据库 表列表" + dest.toString());
        List<String> destLinked = new ArrayList<>();
        List<String> sourceUnLinked = new ArrayList<>();
        Map<String, String> linkedMap = new HashMap<>();
        for (String oracleTableName : source) {
            if (dest.contains(oracleTableName)) {
                linkedMap.put(oracleTableName, oracleTableName);
                destLinked.add(oracleTableName);
                continue;
            }
            String tmp = oracleTableName.replace("KONWLEDGE", "KNOWLEDGE");
            if (dest.contains(tmp)) {
                linkedMap.put(oracleTableName, tmp);
                destLinked.add(tmp);
                continue;
            }
            sourceUnLinked.add(oracleTableName);
        }
        if (dest.contains("TB_ISM_TEMPLATE_SENTENCE")){
            linkedMap.put("TB_ISM_SEMANTIC_TEMPLATE","TB_ISM_TEMPLATE_SENTENCE");
            destLinked.add("TB_ISM_TEMPLATE_SENTENCE");
            sourceUnLinked.remove("TB_ISM_SEMANTIC_TEMPLATE");
        }
        logger.info("原数据库未映射到的表列表：" + sourceUnLinked.toString());
        dest.removeAll(destLinked);
        logger.info("目标数据库未映射到的表列表" + dest.toString());
        return linkedMap;
    }

    /**
     * 查询需要迁移的表的记录数
     *
     * @param tableName
     * @return
     */
    public Long getMoveDataCount(String tableName) {
        Long count = 0L;
        try {

            SQLExecutor sqlExecutor = SQLExecutor.newInstance(sourceDS);
            List<Map<String, Object>> result = sqlExecutor.executeSQL("select count(*) AS NUM from " + tableName);
            if (null != result && result.size() > 0) {
                count = Long.valueOf(result.get(0).get("NUM").toString());
            }
            logger.info("tableName=" + tableName + ",Count=" + count);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("tableName=" + tableName, e);
        }
        return count;
    }

    /**
     * 执行数据库表数据迁移
     *
     * @param sourceTable
     * @param destTable
     */
    public int moveTablesData(String sourceTable, String destTable) {
        String sql = "select * from " + sourceTable;
        return moveTablesData(sourceTable, destTable, sql);
    }

    /**
     * 迁移含树状查询结构的部分表
     *
     * @param sourceTable 源表名
     * @param destTable   目标表名
     * @return
     */
    public int moveTablesDataWithTree(String sourceTable, String destTable) {
        String sql = "SELECT t.*,SYS_CONNECT_BY_PATH(LPAD(t.id,11,'0'), '-') AS TREE_CODE FROM  " +
                sourceTable + " t START WITH   t.parent_id is null CONNECT BY PRIOR t.id = t.parent_id";
        return moveTablesData(sourceTable, destTable, sql);
    }

    /**
     * 执行数据库表数据迁移
     *
     * @param sourceTable
     * @param destTable
     */
    public int moveTablesData(String sourceTable, String destTable, String sourcesql) {
        List<Map<String, Object>> dataList = SQLExecutor.newInstance(sourceDS).executeSQL(sourcesql);
        List<TableColumn> sourceFields = SQLExecutor.newInstance(sourceDS).showTableFields(
                sourceTable, sourceDS.getUser().toUpperCase());
        if (sourcesql.contains("TREE_CODE")){
            sourceFields.add(new TableColumn("TREE_CODE","VARCHAR2"));
        }
        List<TableColumn> destFields = SQLExecutor.newInstance(destDS).showTableFields(
                destTable, null);
        logger.info("源表[" + sourceTable + "],字段数:"+sourceFields.size()+",结构：" + sourceFields.toString());
        logger.info("目标表[" + destTable + "]:字段数:"+destFields.size()+",结构：" + destFields.toString());
        List<MapInfo> linked = preLinkTableColumns(sourceFields, destFields);
        logger.info(sourceTable + "==>" + destTable + ",字段映射:" + linked);
        List<String> sqllist = new ArrayList<>();

        for (int i = 0; i < dataList.size(); i++) {
            sqllist.add(preInsertData(destTable,  linked, dataList.get(i)));
        }
        logger.info("开始迁移：" + sourceTable + "==>" + destTable + "插入条数：" + sqllist.size());
        SQLExecutor.newInstance(destDS).batchExecuteSQL(sqllist,sourceTable + "==>" + destTable);
        return dataList.size();
    }

    /**
     * 对即将插入的数据进行预处理
     *
     * @param destTable  目标数据库表名
     * @param linked        对应的列名
     * @param columnDataMap 待插入数据
     */
    public String preInsertData(String destTable,
                                List<MapInfo> linked,
                                Map<String, Object> columnDataMap) {
        if (null == columnDataMap || columnDataMap.size() <= 0) {
            return null;
        }
        StringBuilder insertSql = new StringBuilder("INSERT INTO ");
        insertSql.append(destTable);
        StringBuilder fields = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (MapInfo mapInfo : linked) {
            Object val = columnDataMap.get(mapInfo.getSource());
            fields.append(mapInfo.getDest()).append(", ");
            if (!columnDataMap.containsKey(mapInfo.getSource()) || val == null) {
                values.append("null, ");
            }else if ("TREE_CODE".equalsIgnoreCase(mapInfo.getSource())) {
                String tmp = val.toString();
                if (tmp.startsWith("-")) {
                    tmp = tmp.substring(1);
                }
                values.append("\'").append(tmp).append("\', ");
            } else if (mapInfo.getSourceType().equalsIgnoreCase("CLOB")) {
                values.append("\'").append(DBUtils.ClobToString((CLOB) val)).append("\', ");
            }else if (mapInfo.getSourceType().contains("TIMESTAMP")) {
                values.append("\'").append(((TIMESTAMP) val).stringValue()).append("\', ");
            } else if (isNeedQuotation(mapInfo.getSourceType())) {
                values.append("\'").append(val.toString().replaceAll("'", "\\\\\'")
                        .replaceAll("\"", "\\\\\"")).append("\', ");
            } else {
                values.append(val).append(", ");
            }
        }
        if( destTable.equalsIgnoreCase("TB_PERMISSION_USER")
                && !fields.toString().contains("business_user_type")){
            fields.append("business_user_type, ");
            values.append("0, ");
        }
        String fieldsStr = fields.toString().trim();
        String valuesStr = values.toString().trim();
        if (fieldsStr.length() > 0) {
            insertSql.append(" (");
            insertSql.append(fieldsStr.substring(0, fieldsStr.length() - 1));
            insertSql.append(")");
            insertSql.append(" VALUES (");
            insertSql.append(valuesStr.substring(0, valuesStr.length() - 1));
            insertSql.append(")");
            return insertSql.toString();
        }

        return null;
    }

    /**
     * 源数据库名列表与表列表的对应映射
     *
     * @param sourceFields
     * @param destFields
     * @return
     */
    public List<MapInfo> preLinkTableColumns(List<TableColumn> sourceFields,
                                             List<TableColumn> destFields) {

        List<String> destLinked = new ArrayList<>();
        List<String> sourceUnLinked = new ArrayList<>();
        List<MapInfo> maplist = new ArrayList<>();
        for (TableColumn tc : sourceFields) {
            boolean isContain = false;
            for (TableColumn desttc : destFields) {
                if (desttc.equals(tc)) {
                    maplist.add(new MapInfo(tc.getName(), desttc.getName(), tc.getType()));
                    destLinked.add(desttc.getName());
                    isContain = true;
                    continue;
                }
            }
            if (!isContain)
                sourceUnLinked.add(tc.getName());
            isContain = false;
        }
        logger.info("源表未映射到的column列表：" + sourceUnLinked.toString());
        logger.info("目标表未映射到的column列表：" + TableColumn.subtract(destFields,destLinked));

        return maplist;
    }


    /**
     * 关闭数据库
     */
    public void close() {
        DBUtils.close(sourceDS);
        DBUtils.close(destDS);
    }
}
