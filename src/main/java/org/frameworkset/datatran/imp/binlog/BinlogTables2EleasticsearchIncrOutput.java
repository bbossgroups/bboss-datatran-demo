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
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
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
public class BinlogTables2EleasticsearchIncrOutput {
    private static Logger logger = LoggerFactory.getLogger(BinlogTables2EleasticsearchIncrOutput.class);
    public static void main(String[] args){
        PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
        //从application.properties文件读取批量入库记录数batchSize，设置默认值500,
        int batchSize = propertiesContainer.getIntSystemEnvProperty("batchSize",500);
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setBatchSize(batchSize);//设置批量入库的记录数
        importBuilder.setFlushInterval(10000L);//如果10秒内没有记录到来，队列中记录数不够500条记录，则强制保存数据到Elasticsearch
        //binlog插件配置开始
        MySQLBinlogConfig mySQLBinlogConfig = new MySQLBinlogConfig();
        mySQLBinlogConfig.setHost("192.168.137.1");
        mySQLBinlogConfig.setPort(3306);
        mySQLBinlogConfig.setDbUser("root");
        mySQLBinlogConfig.setDbPassword("123456");
        mySQLBinlogConfig.setEnableIncrement(true);//开启增量模式
        //如果直接监听文件则设置binlog​文件路径，否则不需要配置文件路径
        //mySQLBinlogConfig.setFileNames("F:\\6_environment\\mysql\\binlog.000107,F:\\6_environment\\mysql\\binlog.000127");
        mySQLBinlogConfig.setTables("cityperson,cityperson1");//监控增量表名称
        mySQLBinlogConfig.setDatabase("bboss");//监控增量表名称
        mySQLBinlogConfig.setServerId(65537L);

        mySQLBinlogConfig.setJoinToConnectTimeOut(5000L);
        //binlog插件配置结束​
        importBuilder.setInputConfig(mySQLBinlogConfig);
        importBuilder.setPrintTaskLog(true);

        ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
        elasticsearchOutputConfig
                .addTargetElasticsearch("elasticsearch.serverNames","default")
                .addElasticsearchProperty("default.elasticsearch.rest.hostNames","192.168.137.1:9200")
                .addElasticsearchProperty("default.elasticsearch.showTemplate","true")
                .addElasticsearchProperty("default.elasticUser","elastic")
                .addElasticsearchProperty("default.elasticPassword","changeme")
                .addElasticsearchProperty("default.elasticsearch.failAllContinue","true")
                .addElasticsearchProperty("default.http.timeoutSocket","60000")
                .addElasticsearchProperty("default.http.timeoutConnection","40000")
                .addElasticsearchProperty("default.http.connectionRequestTimeout","70000")
                .addElasticsearchProperty("default.http.maxTotal","200")
                .addElasticsearchProperty("default.http.defaultMaxPerRoute","100")
//                .setIndex("other-{dateformat=yyyy.MM.dd}")
                .setIndex("other")
                .setRefreshOption("refresh=true")//为了演示效果，打开es写入强制刷新机制
                .setEsIdField("rowNo");//设置文档主键，不设置，则自动产生文档id,设置好id后，binlog采集的增删改数据，会自动同步到Elasticsearch
        importBuilder.setOutputConfig(elasticsearchOutputConfig);

        importBuilder.setStatusDbname("testStatus");//指定增量状态数据源名称
        importBuilder.setLastValueStorePath("binlogtables");


        importBuilder.setDataRefactor(new DataRefactor() {
            @Override
            public void refactor(Context context) throws Exception {
                int action = (int)context.getMetaValue("action");
                if(context.isUpdate()) {
//                    context.setDrop(true); //丢弃修改和删除数据
                    logger.info("isUpdate");
                }

                if(context.isDelete()) {
//                    context.setDrop(true); //丢弃修改和删除数据
                    logger.info("isDelete");
                }

                if(context.isInsert()) {
//                    context.setDrop(true); //丢弃修改和删除数据
                    logger.info("isInsert");
                }

                //将不同的表数据的增删改数据保存到不同的Elasticsearch索引表
                String table = (String)context.getMetaValue("table");
                if(table.equals("cityperson"))
                    context.setIndex("cityperson");
                else  if(table.equals("cityperson1")) {
                    context.setIndex("cityperson1");
                    context.addFieldValue("remark","cityperson1");
                    context.addFieldValue("person_no","430525198811192606");
                    context.addIgnoreFieldMapping("zhs_item");
                }
                else
                    context.setIndex("other");
//                int action1 = (int)context.getMetaValue("action1");
            }
        });
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();


    }
}
