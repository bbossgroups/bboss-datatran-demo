package org.frameworkset.datatran.imp.binlog;
/**
 * Copyright 2023 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.spi.assemble.PropertiesContainer;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.db.output.SQLConf;
import org.frameworkset.tran.plugin.mysqlbinlog.input.MySQLBinlogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/5/18
 * @author biaoping.yin
 * @version 1.0
 */
public class MasterSlaveBinlog2DBOutputUnIncre {
    private static Logger logger = LoggerFactory.getLogger(MasterSlaveBinlog2DBOutputUnIncre.class);
    public static void main(String[] args){
        PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
        int batchSize = propertiesContainer.getIntSystemEnvProperty("batchSize",500);//同时指定了默认值
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setBatchSize(batchSize);//设置批量入库的记录数

        MySQLBinlogConfig mySQLBinlogConfig = new MySQLBinlogConfig();
        mySQLBinlogConfig.setHost("192.168.137.1");
        mySQLBinlogConfig.setPort(3306);
        mySQLBinlogConfig.setDbUser("root");
        mySQLBinlogConfig.setDbPassword("123456");
        mySQLBinlogConfig.setServerId(100000L);
        mySQLBinlogConfig.setTables("cityperson,batchtest");//
        mySQLBinlogConfig.setDatabase("bboss");
        mySQLBinlogConfig.setEnableIncrement(false);
        importBuilder.setInputConfig(mySQLBinlogConfig);
        importBuilder.setPrintTaskLog(true);



        DBOutputConfig dbOutputConfig = new DBOutputConfig();
        dbOutputConfig
                .setDbName("test")
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") 
                .setDbUser("root")
                .setDbPassword("123456")
                .setValidateSQL("select 1")
                .setUsePool(true)
                .setDbInitSize(5)
                .setDbMinIdleSize(5)
                .setDbMaxSize(10)
                .setShowSql(true)//是否使用连接池;
                .setSqlFilepath("dsl2ndSqlFile.xml");//sql语句配置文件路径

        //设置不同表对应的增删改sql语句
        SQLConf sqlConf = new SQLConf();
        sqlConf.setInsertSqlName("insertcitypersonSQL");//对应sql配置文件dsl2ndSqlFile.xml配置的sql语句insertcitypersonSQL
//        sqlConf.setUpdateSqlName("citypersonUpdateSQL");//可选
//        sqlConf.setDeleteSqlName("citypersonDeleteSQL");//可选
        dbOutputConfig.addSQLConf("cityperson",sqlConf);

        sqlConf = new SQLConf();
        sqlConf.setInsertSqlName("insertbatchtestSQL");//对应sql配置文件dsl2ndSqlFile.xml配置的sql语句insertbatchtestSQL
//        sqlConf.setUpdateSqlName("batchtestUpdateSQL");//可选
//        sqlConf.setDeleteSqlName("batchtestDeleteSQL");//可选
        dbOutputConfig.addSQLConf("batchtest",sqlConf);
        importBuilder.setOutputConfig(dbOutputConfig);
        importBuilder.setDataRefactor(new DataRefactor() {
            @Override
            public void refactor(Context context) throws Exception {
                int action = (int)context.getMetaValue("action");
                if(context.isUpdate() || context.isDelete())
                    context.setDrop(true); //丢弃修改和删除数据
//                int action1 = (int)context.getMetaValue("action1");
            }
        });
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();
    }
}
