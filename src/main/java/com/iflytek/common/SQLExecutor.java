package com.iflytek.common;


import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 功能：SQL执行器
 *
 * @author miaojundong
 */
public class SQLExecutor {

    private Logger logger = LogUtils.getLogger(SQLExecutor.class);

    private DataSource dataSource;

    private SQLExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static SQLExecutor newInstance(DataSource dataSource) {
        return new SQLExecutor(dataSource);
    }


    /**
     * 查询数据库表名
     *
     * @return
     */
    public List<String> showTables(String schemaPattern) {
        List<String> list = new ArrayList<String>();

        ResultSet rs = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData dmd = connection.getMetaData();
            rs = dmd.getTables(null, schemaPattern, "%", new String[]{"TABLE"});
            while (rs.next()) {
                list.add(rs.getString("TABLE_NAME").toUpperCase());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtils.close(rs, null, connection);
        }
        return list;
    }

    /**
     * 查询表字段信息
     *
     * @param tableName
     */
    public List<TableColumn> showTableFields(String tableName, String schemaPattern) {
        List<TableColumn> list = new ArrayList<>();
        ResultSet rs = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData dmd = connection.getMetaData();
            rs = dmd.getColumns(null, schemaPattern, tableName.toUpperCase(), "%");

            while (rs.next()) {
                TableColumn tc = new TableColumn();
                tc.setName(rs.getString("COLUMN_NAME"));
                tc.setType(rs.getString("TYPE_NAME"));
                list.add(tc);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtils.close(rs, null, connection);
        }
        return list;
    }

    /**
     * 执行查询SQL
     *
     * @param sql
     * @return
     */
    public List<Map<String, Object>> executeSQL(String sql) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        List<Map<String, Object>> list = new ArrayList<>();

        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Object> map = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(metaData.getColumnLabel(i), rs.getObject(i));
                }
                list.add(map);
            }

            stopWatch.stop();
            logger.info("sql语句：{}，耗时: {}s", sql, stopWatch.elapsed(TimeUnit.SECONDS));
        } catch (Exception e) {
            logger.error("sql语句执行出错：" + sql, e);
        } finally {
            DBUtils.close(rs, ps, connection);
        }
        return list;
    }

    /**
     * 删除表内容
     *
     * @param tableName
     */
    public void clearTable(String tableName) {
        Connection connection = null;
        Statement st = null;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            connection = dataSource.getConnection();
            st = connection.createStatement();
            String sql = "delete from " + tableName;
            int ret = st.executeUpdate(sql);
            stopWatch.stop();
            logger.info("sql语句：{}，影响行数：{}行,耗时: {}s", sql, ret,
                    stopWatch.elapsed(TimeUnit.SECONDS));
        } catch (Exception e) {
            logger.error("sql语句执行出错：delete from  " + tableName, e);
        } finally {
            DBUtils.close(null, st, connection);
        }
    }

    /**
     * 批量执行更新SQL
     *
     * @param sqlList
     * @return
     */
    public int batchExecuteSQL(List<String> sqlList,String desc) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int result = 0;
        Statement ps = null;
        int i = 0;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            ps = connection.createStatement();
            int currentCount = 0;
            int index = 0;
            for (String sql : sqlList) {
                // 加入SQL队列
                ps.addBatch(sql);
                i++;
                currentCount++;
                // 每10000条SQL提交一次
                if (i % 1000 == 0) {
                    ps.executeBatch();
                    connection.commit();
                    index++;
                    logger.info(desc+"执行第{}次提交, 成功{}条", index, currentCount);
                    currentCount = 0;
                }
            }
            // 若总条数不是批量数值的整数倍, 则还需要再额外的执行一次.
            if (i % 1000 != 0) {
                index++;
                ps.executeBatch();
                connection.commit();
                logger.info(desc+"执行第{}次提交, 成功{}条", index, currentCount);
            }

            result = 1;
            stopWatch.stop();
            logger.info(desc+"执行添加时间: {}s", stopWatch.elapsed(TimeUnit.SECONDS));
        } catch (Exception e) {
            result = -1;
            logger.error(desc+"sql语句执行失败", e);
            e.printStackTrace();
        } finally {
            DBUtils.close(null, ps, connection);
        }
        return result;
    }
}
