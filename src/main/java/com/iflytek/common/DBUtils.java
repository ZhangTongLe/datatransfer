package com.iflytek.common;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import oracle.sql.CLOB;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBUtils {

    /**
     * 创建源数据库链接池
     *
     * @param properties
     * @return
     * @throws Exception
     */
    public static final ComboPooledDataSource createSourceDatasource(Properties properties) throws Exception {
        String driver = properties.getProperty(Constants.SourceDB.DRIVER);
        String url = properties.getProperty(Constants.SourceDB.URL);
        String username = properties.getProperty(Constants.SourceDB.USERNAME);
        String passwd = properties.getProperty(Constants.SourceDB.PASSWD);
        int poolsize = Integer.valueOf(properties.getProperty(Constants.THREAD_NUM, "5"));
        return DBUtils.getDataSource(driver, url, username, passwd, poolsize);
    }

    /**
     * 创建目标数据库链接池
     *
     * @param properties
     * @return
     * @throws Exception
     */
    public static final ComboPooledDataSource createDestDatasource(Properties properties) throws Exception {
        String driver = properties.getProperty(Constants.DestDB.DRIVER);
        String url = properties.getProperty(Constants.DestDB.URL);
        String username = properties.getProperty(Constants.DestDB.USERNAME);
        String passwd = properties.getProperty(Constants.DestDB.PASSWD);
        int poolsize = Integer.valueOf(properties.getProperty(Constants.THREAD_NUM, "5"));
        return DBUtils.getDataSource(driver, url, username, passwd, poolsize);
    }

    /**
     * 获取alibaba druid数据库连接池链接
     *
     * @param driverClassName
     * @param url
     * @param username
     * @param passwd
     * @return
     * @throws Exception
     */
    public static final ComboPooledDataSource getDataSource(
            String driverClassName, String url, String username, String passwd, int poolsize) throws Exception {
        ComboPooledDataSource cds = new ComboPooledDataSource();
        cds.setDriverClass(driverClassName);
        cds.setJdbcUrl(url);
        cds.setUser(username);
        cds.setPassword(passwd);
        cds.setMaxPoolSize(poolsize);
        cds.setMinPoolSize(poolsize);
        cds.setMaxStatements(10);
        //当连接池中的连接耗尽的时候c3p0一次同时获取的连接数
        cds.setAcquireIncrement(3);

        //定义在从数据库获取新连接失败后重复尝试的次数
        cds.setAcquireRetryAttempts(60);
        //两次连接中间隔时间，单位毫秒
        cds.setAcquireRetryDelay(1000);
        //连接关闭时默认将所有未提交的操作回滚
        cds.setAutoCommitOnClose(false);
        //当连接池用完时客户端调用getConnection()后等待获取新连接的时间，超时后将抛出SQLException,如设为0则无限期等待。单位毫秒
        cds.setCheckoutTimeout(3000);
        //每120秒检查所有连接池中的空闲连接。Default: 0
        cds.setIdleConnectionTestPeriod(120);
        //最大空闲时间,60秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0
        cds.setMaxIdleTime(600);
        return cds;
    }


    /**
     * 关闭druidDataSource
     *
     * @param druidDataSource
     */
    public static void close(ComboPooledDataSource druidDataSource) {
        if (druidDataSource != null) {
            druidDataSource.close();
        }
    }

    /**
     * oracle CLOB转string
     *
     * @param clob
     * @return
     */
    public static String ClobToString(CLOB clob)  {

        String reString = "";
        try {
            java.io.Reader is = clob.getCharacterStream();// 得到流
            BufferedReader br = new BufferedReader(is);
            String s = br.readLine();
            StringBuffer sb = new StringBuffer();
            while (s != null) {
                sb.append(s);
                s = br.readLine();
            }
            reString = sb.toString().replaceAll("'", "\\\\\'")
                    .replaceAll("\"", "\\\\\"");
        }catch (Exception e){
            e.printStackTrace();
        }
        return reString;
    }


    /**
     * 关流
     *
     * @param rs
     * @param st
     * @param conn
     */
    public static void close(ResultSet rs, Statement st, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
