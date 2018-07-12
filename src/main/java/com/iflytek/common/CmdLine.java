package com.iflytek.common;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Logger;


/**
 * 命令行属性类
 */
public class CmdLine {
    private static Logger logger = LogUtils.getLogger(CmdLine.class);

    /**
     * 是否先清楚目标数据库表内容
     */
    private boolean isPreClear;
    /**
     * 配置路径
     */
    private String path;
    /**
     * 数据库类型
     */
    private String type;

    public boolean isPreClear() {
        return isPreClear;
    }

    public void setPreClear(boolean preClear) {
        isPreClear = preClear;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {

        this.path = path ;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type == null ? type : type.toUpperCase();
    }

    @Override
    public String toString() {
        return "CmdLine{" +
                "isPreClear=" + isPreClear +
                ", path='" + path + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public static CmdLine parse(String args[]){
        try {
            // create Options object
            Options options = new Options();
            options.addOption(new Option("c", "clear", false, "预先清空目标数据库内容"));
            options.addOption(new Option("t", "type", true, "转移数据库类型（ISM/PERMISSION）"));
            options.addOption(new Option("p", "configpath", true, "数据库配置文件路径"));

            // print usage
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "database transfer tool CLI", options );
            System.out.println();

            // create the command line parser
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse(options, args);

            // check the options have been set correctly
            CmdLine cl = new CmdLine();
            cl.setPath(cmd.getOptionValue("p"));
            cl.setType(cmd.getOptionValue("t"));
            cl.setPreClear(cmd.hasOption("c"));

            logger.info(cl.toString());
            return cl;
        } catch (Exception ex) {
            logger.error("database transfer tool CLI error:",ex);
            return  null;
        }
    }

    /**
     * 命令行参数检测
     * @return
     */
    public   boolean validateCmdLine(){
        if (this.getType() == null || this.getPath() == null){
            return false;
        }
        if (!("PERMISSION".equalsIgnoreCase(this.getType()) || "ISM".equalsIgnoreCase(this.getType()))){
            return false;
        }
        return  true;
    }
}
