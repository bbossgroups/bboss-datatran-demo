package org.frameworkset.datatran.imp.rocketmq;
/**
 * Copyright 2024 bboss
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
import org.frameworkset.datatran.imp.Db2DBdemo;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.rocketmq.input.RocketmqInputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/21
 */
public class Rockemq2Custom {

    private static final Logger logger = LoggerFactory.getLogger(Rockemq2Custom.class);
    public static void main(String[] args){
        Rockemq2Custom rockemq2Custom = new Rockemq2Custom();
//		dbdemo.fullImportData(  dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
        rockemq2Custom.scheduleImportData();
//		dbdemo.scheduleImportData(dropIndice);
    }

    /**
     * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
     */
    public void scheduleImportData(){
        ImportBuilder importBuilder = ImportBuilder.newInstance();
        importBuilder.setJobId("Rockemq2Custom");
        /**
         * 设置增量状态ID生成策略，在设置jobId的情况下起作用
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用[jobType]+[jobId]+[作业查询语句/文件路径等信息的hashcode]，作为增量id作为增量状态id
         * 默认值ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT
         */
        importBuilder.setStatusIdPolicy(ImportIncreamentConfig.STATUSID_POLICY_JOBID);


        //指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
        // 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
        // select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
        // 需要设置setLastValueColumn信息log_id，
        // 通过setLastValueType方法告诉工具增量字段的类型，默认是数字类型


        /**
         * Rocketmq输入插件配置
         */
        RocketmqInputConfig rocketmqInputConfig = new RocketmqInputConfig();
        rocketmqInputConfig.setNamesrvAddr("172.24.176.18:9876") //Rocketmq服务器地址
                .setConsumerGroup("etlgroup2")  //消费组
                .setTopic("etltopic")  //消费主题
                .setTag("json") //指定消费过滤tag，参考Rocketmq官方文档配置
                .setAccessKey("Rocketmq") 
                .setSecretKey("12345678").setMaxPollRecords(100)
                .setConsumeMessageBatchMaxSize(50)
                /**
                 *      * CONSUME_FROM_LAST_OFFSET,
                 *      *
                 *      *     @Deprecated
                 *      *     CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
                 *      *     @Deprecated
                 *      *     CONSUME_FROM_MIN_OFFSET,
                 *      *     @Deprecated
                 *      *     CONSUME_FROM_MAX_OFFSET,
                 *      *     CONSUME_FROM_FIRST_OFFSET,
                 *      *     CONSUME_FROM_TIMESTAMP,
                 */
                .setConsumeFromWhere("CONSUME_FROM_FIRST_OFFSET")
                .setWorkThreads(10)//并行消费线程数
                .setKeyDeserializer("org.frameworkset.rocketmq.codec.StringCodecDeserial") //消息key解码器
                .setValueDeserializer("org.frameworkset.rocketmq.codec.JsonMapCodecDeserial");//消息解码器
        importBuilder.setInputConfig(rocketmqInputConfig);

        //自己处理数据
        CustomOutputConfig customOutputConfig = new CustomOutputConfig();
        customOutputConfig.setCustomOutPut(new CustomOutPut() {
            @Override
            public void handleData(TaskContext taskContext, List<CommonRecord> datas) {

                //You can do any thing here for datas
                for(CommonRecord record:datas){
                    Map<String,Object> data = record.getDatas();
                    logger.info(SimpleStringUtil.object2json(data));
                    logger.info(SimpleStringUtil.object2json(record.getMetaDatas()));

                }
            }
        });
        importBuilder.setOutputConfig(customOutputConfig);

        importBuilder.setBatchSize(10); //可选项,批量导入db的记录数，默认为-1，逐条处理，> 0时批量处理
        //定时任务配置，
        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
                .setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
                .setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
        //定时任务配置结束
//

  
        /**
         * 重新设置数据结构
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception  {
                //可以根据条件定义是否丢弃当前记录
                //context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
                //获取元数据
                String topic = (String)context.getMetaValue("topic");

                context.addFieldValue("author","duoduo");
               
            }
        });
        //映射和转换配置结束
        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

        importBuilder.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
        importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
            @Override
            public void success(TaskCommand<String> taskCommand, String result) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                logger.info(taskMetrics.toString());
            }

            @Override
            public void error(TaskCommand<String> taskCommand, String result) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                logger.info(taskMetrics.toString());
            }

            @Override
            public void exception(TaskCommand<String> taskCommand, Throwable exception) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                logger.info(taskMetrics.toString());
            }


        });
        /**
         * 执行作业
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//执行导入操作



    }

}
