package org.frameworkset.elasticsearch.imp.binlog;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.mysqlbinlog.input.MySQLBinlogConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/5/18
 * @author biaoping.yin
 * @version 1.0
 */
public class MasterHistoryBinlog2CustomOutput {
    private static Logger logger = LoggerFactory.getLogger(MasterHistoryBinlog2CustomOutput.class);
    public static void main(String[] args){
        PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
        int batchSize = propertiesContainer.getIntSystemEnvProperty("batchSize",500);//同时指定了默认值
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setBatchSize(batchSize);//设置批量入库的记录数

        MySQLBinlogConfig mySQLBinlogConfig = new MySQLBinlogConfig();
        mySQLBinlogConfig.setHost("localhost");
        mySQLBinlogConfig.setPort(3306);
        mySQLBinlogConfig.setDbUser("root");
        mySQLBinlogConfig.setDbPassword("123456");
        mySQLBinlogConfig.setServerId(100000L);
        mySQLBinlogConfig.setTables("cityperson,batchtest");//
        mySQLBinlogConfig.setDatabase("bboss");
        mySQLBinlogConfig.setCollectMasterHistoryBinlog(true);
        mySQLBinlogConfig.setBinlogDir("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Data");
//        mySQLBinlogConfig.setIncreamentFrom001(true);

        importBuilder.setInputConfig(mySQLBinlogConfig);
        importBuilder.setPrintTaskLog(true);
        importBuilder.setContinueOnError(false);

//        importBuilder.setStatusDbname("testStatus");//指定增量状态数据源名称
		importBuilder.setLastValueStorePath("historybinlog2custom_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        importBuilder.setLastValueStoreTableName("binlog");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab

        //自己处理数据
        CustomOutputConfig customOutputConfig = new CustomOutputConfig();
        customOutputConfig.setCustomOutPut(new CustomOutPut() {
            @Override
            public void handleData(TaskContext taskContext, List<CommonRecord> datas) {

                //You can do any thing here for datas
                for(CommonRecord record:datas){
                    Map<String,Object> data = record.getDatas();//获取原始数据记录，key/vlaue，代表字段和值
                    int action = (int)record.getMetaValue("action");//获取元数据,记录类型，可以是新增（默认类型）/修改/删除/其他类型
                    String table = (String)record.getMetaValue("table");//获取元数据中的表名称
                    logger.info("data:{},action:{},record action type:insert={},update={},delete={}",data,action,record.isInsert(),record.isUpdate(),record.isDelete());

                    logger.info(SimpleStringUtil.object2json(record.getMetaDatas()));//获取并打印所有元数据信息
                    if(record.isDelete()){
                        logger.info("record.isDelete");
                    }

                    if(record.isUpdate()){
                        logger.info("record.isUpate");
                        Map<String,Object> oldDatas = record.getUpdateFromDatas();
                    }
//                    logger.info(SimpleStringUtil.object2json(data));
                }
            }
        });
        importBuilder.setDataRefactor(new DataRefactor() {
            @Override
            public void refactor(Context context) throws Exception {
                int action = (int)context.getMetaValue("action");
                String table = (String)context.getMetaValue("table");
//                int action1 = (int)context.getMetaValue("action1");
            }
        });
        importBuilder.addCallInterceptor(new CallInterceptor() {
            @Override
            public void preCall(TaskContext taskContext) {

            }

            @Override
            public void afterCall(TaskContext taskContext) {

            }

            @Override
            public void throwException(TaskContext taskContext, Throwable e) {
                logger.error("",e);
            }
        });
        importBuilder.setOutputConfig(customOutputConfig);
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();
    }
}
