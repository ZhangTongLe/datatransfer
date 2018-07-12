package com.iflytek;

import org.apache.commons.cli.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Date;

public class TestUtil {
    @Test
    public void test1() throws Exception {
        String driverClassName = "";
        String url = "jdbc:oracle:thin:@192.168.77.122:1521:orcl";
        String usrName = "simba_ism";
        String password = "iflytek";
        Class.forName("oracle.jdbc.OracleDriver");

        Connection conn = DriverManager.getConnection(url, usrName, password);
        System.out.println("1");
    }

    @Test
    public void testCmd(){
        System.out.println("KONWLEDGE_point_id".replaceAll("KONWLEDGE|konwledge", "KNOWLEDGE"));
    }

}
