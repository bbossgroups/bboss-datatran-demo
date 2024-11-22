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
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.rocketmq.output.RocketmqOutputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.frameworkset.tran.context.Context.ROCKETMQ_TOPIC_KEY;


/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/22
 */
public class DB2Rocketmq {
    private static Logger logger = LoggerFactory.getLogger(DB2Rocketmq.class);
    public static void main(String[] args){
        DB2Rocketmq db2Rocketmq = new DB2Rocketmq();
        db2Rocketmq.scheduleImportData();
    }
    /**
     * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
     */
    public void scheduleImportData(){
        ImportBuilder importBuilder = ImportBuilder.newInstance();
        importBuilder.setJobId("DB2Rocketmq");
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
         * 源db相关配置
         */
        DBInputConfig dbInputConfig = new DBInputConfig();
        dbInputConfig.setDbName("source")
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
                .setDbUser("root")
                .setDbPassword("123456")
                .setValidateSQL("select 1")
                .setUsePool(true)//是否使用连接池
                .setSqlFilepath("sql.xml")
                .setSqlName("demoexport").setParallelDatarefactor(true);
        importBuilder.setInputConfig(dbInputConfig);

        RocketmqOutputConfig rocketmqOutputConfig = new RocketmqOutputConfig();
        rocketmqOutputConfig.setNamesrvAddr("172.24.176.18:9876")
                .setProductGroup("etlgroup2")
                .setTopic("etltopic")//全局topic
                .setTag("json").setAccessKey("Rocketmq")
                .setSecretKey("12345678");
        rocketmqOutputConfig.setValueCodecSerial("org.frameworkset.rocketmq.codec.StringBytesCodecSerial")
                .setKeyCodecSerial("org.frameworkset.rocketmq.codec.StringCodecSerial") ;
        
        importBuilder.setOutputConfig(rocketmqOutputConfig);

       
        importBuilder.setBatchSize(10); //可选项,批量导入db的记录数，默认为-1，逐条处理，> 0时批量处理
        //定时任务配置，
        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
                .setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
                .setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
        //定时任务配置结束
 
//		//设置任务执行拦截器结束，可以添加多个
        //增量配置开始
//		importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
//		importBuilder.setDateLastValueColumn("log_id");//手动指定日期增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
        importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
        //setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
        importBuilder.setLastValueStorePath("DB2Rocketmq_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
        importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
        // 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
        //增量配置结束
        
       
        /**
         * 设置数据结构和消息key以及记录级别topic
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception  {
                //可以根据条件定义是否丢弃当前记录
                //context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}

                //设置消息key
                context.setMessageKey("testKey");

                //设置消息发送的主题
                context.addTempData(Context.ROCKETMQ_TOPIC_KEY,"othertopic");
                
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

        importBuilder.setUseLowcase(false)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
                .setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
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
         * 执行数据库表数据导入Rocketmq操作
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//执行导入操作

        logger.info("come to end.");


    }

}
