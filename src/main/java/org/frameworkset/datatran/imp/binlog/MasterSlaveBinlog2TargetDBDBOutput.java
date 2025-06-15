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

import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.DBStartResult;
import com.frameworkset.common.poolman.util.SQLManager;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.listener.AsynJobClosedListener;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.db.output.DDLConf;
import org.frameworkset.tran.plugin.db.output.DatabaseTableSqlConfResolver;
import org.frameworkset.tran.plugin.db.output.SQLConf;
import org.frameworkset.tran.plugin.mysqlbinlog.input.MySQLBinlogConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
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
public class MasterSlaveBinlog2TargetDBDBOutput {
    private static Logger logger = LoggerFactory.getLogger(MasterSlaveBinlog2TargetDBDBOutput.class);
    public static void main(String[] args){
        PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
        int batchSize = 1;//propertiesContainer.getIntSystemEnvProperty("batchSize",1);//同时指定了默认值
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setBatchSize(batchSize);//设置批量入库的记录数

        //通过作业初始化配置，对作业运行过程中依赖的数据源等资源进行初始化
        importBuilder.setImportStartAction(new ImportStartAction() {
            /**
             * 初始化之前执行的处理操作，比如后续初始化操作、数据处理过程中依赖的资源初始化
             * @param importContext
             */
            @Override
            public void startAction(ImportContext importContext) {


                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {

                        ResourceStartResult resourceStartResult = null;

                        DBConf tempConf = new DBConf();
                        tempConf.setPoolname("ddlsyn");//用于验证ddl同步处理的数据源
                        tempConf.setDriver("com.mysql.cj.jdbc.Driver");
                        tempConf.setJdbcurl("jdbc:mysql://192.168.137.1:3306/pinpoint?allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true");

                        tempConf.setUsername("root");
                        tempConf.setPassword("123456");
                        tempConf.setValidationQuery("select 1");

                        tempConf.setInitialConnections(5);
                        tempConf.setMinimumSize(10);
                        tempConf.setMaximumSize(10);
                        tempConf.setUsepool(true);
                        tempConf.setShowsql(true);
                        tempConf.setJndiName("ddlsyn-jndi");
                        //# 控制map中的列名采用小写，默认为大写
                        tempConf.setColumnLableUpperCase(false);
                        //启动数据源
                        boolean result = SQLManager.startPool(tempConf);
                        //记录启动的数据源信息，用户作业停止时释放数据源
                        if(result){
                            if(resourceStartResult == null)
                                resourceStartResult = new DBStartResult();
                            resourceStartResult.addResourceStartResult("ddlsyn");
                        }

                        return resourceStartResult;
                    }
                });

            }

            /**
             * 所有初始化操作完成后，导出数据之前执行的操作
             * @param importContext
             */
            @Override
            public void afterStartAction(ImportContext importContext) {

            }
        });

        //任务结束后销毁初始化阶段初始化的数据源等资源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {
                //销毁初始化阶段自定义的数据源
                importContext.destroyResources(new ResourceEnd() {
                    @Override
                    public void endResource(ResourceStartResult resourceStartResult) {
                        if(resourceStartResult instanceof DBStartResult) { //作业停止时，释放db数据源
                            DataTranPluginImpl.stopDatasources((DBStartResult) resourceStartResult);
                        }
                    }
                });
            }
        });
        MySQLBinlogConfig mySQLBinlogConfig = new MySQLBinlogConfig();
        mySQLBinlogConfig.setHost("192.168.137.1");
        mySQLBinlogConfig.setPort(3306);
        mySQLBinlogConfig.setDbUser("root");
        mySQLBinlogConfig.setDbPassword("123456");
        mySQLBinlogConfig.setServerId(100001L);
        //ddl同步配置，将bboss和visualops两个数据库的ddl操作在test和ddlsyn数据源上进行回放
        mySQLBinlogConfig.setDdlSyn(true);
        mySQLBinlogConfig.setDdlSynDatabases("bboss,visualops");

        mySQLBinlogConfig.setTables("bboss.cityperson,visualops.batchtest");//指定要同步的表，多个用逗号分隔，表名前可以追加数据库名称,格式为:dbname.tablename
//        mySQLBinlogConfig.setDatabase("bboss,visualops");指定需要同步的数据库清单，多个用逗号分隔
        mySQLBinlogConfig.setEnableIncrement(true);

        importBuilder.setInputConfig(mySQLBinlogConfig);
        importBuilder.setPrintTaskLog(true);
        importBuilder.setContinueOnError(false);

//        importBuilder.setStatusDbname("testStatus");//指定增量状态数据源名称
		importBuilder.setLastValueStorePath("binlog2db_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        importBuilder.setLastValueStoreTableName("binlog");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab

        DBOutputConfig dbOutputConfig = new DBOutputConfig();
        dbOutputConfig
                .setDbName("test")
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://192.168.137.1:3306/apm?allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") 
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
        sqlConf.setUpdateSqlName("citypersonUpdateSQL");//可选
        sqlConf.setDeleteSqlName("citypersonDeleteSQL");//可选
        sqlConf.setTargetDbName("test,ddlsyn");//为不同的库表sql配置指定对应的目标数据源，多个用逗号分隔，如果不指定就采用dbOutputConfig.setDbName方法设置的数据源
        dbOutputConfig.addSQLConf("bboss.cityperson",sqlConf);//数据库加表名称保存sql配置，对应的sql在sqlconf指定的数据源test上执行

        sqlConf = new SQLConf();
        sqlConf.setInsertSqlName("insertbatchtest1SQL");//对应sql配置文件dsl2ndSqlFile.xml配置的sql语句insertbatchtestSQL
        sqlConf.setUpdateSqlName("batchtest1UpdateSQL");//可选
        sqlConf.setDeleteSqlName("batchtest1DeleteSQL");//可选
        sqlConf.setTargetDbName("test,ddlsyn");//多个用逗号分隔
        dbOutputConfig.addSQLConf("visualops.batchtest",sqlConf);
        dbOutputConfig.setSqlConfResolver(new DatabaseTableSqlConfResolver());


        DDLConf ddlConf = new DDLConf();
        ddlConf.setDatabase("visualops");
        ddlConf.setTargetDbName("ddlsyn,test");//database visualops的ddl同步目标数据源，多个用逗号分隔

        dbOutputConfig.addDDLConf(ddlConf);
        ddlConf = new DDLConf();
        ddlConf.setDatabase("bboss");
        ddlConf.setTargetDbName("ddlsyn,test");//database bboss的ddl同步目标数据源，多个用逗号分隔
        dbOutputConfig.addDDLConf(ddlConf);
        dbOutputConfig.setIgnoreDDLSynError(true);//忽略ddl回放异常，如果ddl已经执行过，可能会报错，忽略sql执行异常

        importBuilder.setOutputConfig(dbOutputConfig);
        importBuilder.setDataRefactor(new DataRefactor() {
            @Override
            public void refactor(Context context) throws Exception {
                int action = (int)context.getMetaValue("action");
//                if(context.isUpdate() || context.isDelete())
//                    context.setDrop(true); //丢弃修改和删除数据
                String database = (String)context.getMetaValue("database");
                if( context.isDDL()) {
                    String ddl = context.getStringValue("ddl").trim().toLowerCase();
                    logger.info(context.getStringValue("ddl"));
                    logger.info(context.getStringValue("errorCode"));
                    logger.info(context.getStringValue("executionTime"));
                    boolean isddl = ddl.indexOf("create ") > 0 || ddl.indexOf("alter ") > 0 || ddl.indexOf("drop ") > 0;
                    if(!isddl){
                        context.setDrop(true);
                    }


                }
                logger.info("database:{}",(String)context.getMetaValue("database"));
//                int action1 = (int)context.getMetaValue("action1");
            }
        });
        /**
        //同步执行JobClosedListener
        importBuilder.setJobClosedListener(new JobClosedListener() {
            @Override
            public void jobClosed(ImportContext importContext, Throwable throwable) {
                if(throwable != null) {
                    logger.info("Job Closed by exception:",throwable);
                }
                else{
                    logger.info("Job Closed normal.");
                }

            }
        });*/

        //异步执行JobClosedListener
        importBuilder.setJobClosedListener(new AsynJobClosedListener() {
            @Override
            public void jobClosed(ImportContext importContext, Throwable throwable) {
                if(throwable != null) {
                    logger.info("Job Closed by exception:",throwable);
                }
                else{
                    logger.info("Job Closed normal.");
                }

            }
        });
        importBuilder.setExportResultHandler(new ExportResultHandler() {
            @Override
            public void success(TaskCommand taskCommand, Object o) {

            }

            @Override
            public void error(TaskCommand taskCommand, Object o) {

            }

            @Override
            public void exception(TaskCommand taskCommand, Throwable exception) {
                logger.warn("exception come:",exception);
            }
        });
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();
    }
}
