# datatransfer  
- 数据迁移工具，当前支持oracle 到 mysql
- 使用jdbc的方式链接原数据库与目标数据库
- 使用了c3p0数据库连接池与线程池，优化数据库访问与并发效率
- 使用apache cli 命令行工具，实现命令行参数
- 使用命令：java -jar ./datatransfer-1.0-SNAPSHOT-jar-with-dependencies.jar -t ISM -c -p ism.properties
