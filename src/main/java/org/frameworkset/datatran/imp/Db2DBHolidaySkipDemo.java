package org.frameworkset.datatran.imp;
/**
 * Copyright 2008 biaoping.yin
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

import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.timer.HolidayScheduleConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Date;

/**
 * <p>Description: 同步处理程序，如需调试同步功能，
 * 请运行测试用例DbdemoTest中调试</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2DBHolidaySkipDemo {
	private static final Logger logger = LoggerFactory.getLogger(Db2DBHolidaySkipDemo.class);
	public static void main(String[] args){
		Db2DBHolidaySkipDemo dbdemo = new Db2DBHolidaySkipDemo();
//		dbdemo.fullImportData(  dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
		dbdemo.scheduleImportData();
//		dbdemo.scheduleImportData(dropIndice);
	}

	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleImportData(){
		ImportBuilder importBuilder = ImportBuilder.newInstance();
        importBuilder.setJobId("Db2DBdemo");
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
//                .setDbAdaptor("com.frameworkset.orm.adapter.DBOceanbase")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true")
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(true)//是否使用连接池
				.setSqlFilepath("sql.xml")
				.setSqlName("demoexport").setParallelDatarefactor(true);
		importBuilder.setInputConfig(dbInputConfig);

		DBOutputConfig dbOutputConfig = new DBOutputConfig();
		dbOutputConfig.setDbName("target")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true")
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(true)//是否使用连接池
				.setSqlFilepath("sql.xml")
				.setInsertSqlName("insertSql");
		importBuilder.setOutputConfig(dbOutputConfig);

	 
		importBuilder.setBatchSize(10); //可选项,批量导入db的记录数，默认为-1，逐条处理，> 0时批量处理
		//定时任务配置
        HolidayScheduleConfig holidayScheduleConfig = new HolidayScheduleConfig();

        holidayScheduleConfig.setFixedRate(false);//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
        holidayScheduleConfig.setDelay(1000L); // 任务延迟执行deylay毫秒后执行
        holidayScheduleConfig.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
        holidayScheduleConfig.addCustomHoliday("2026-06-10");
        importBuilder.setScheduleConfig(holidayScheduleConfig);
		//定时任务配置结束
 
//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
//		importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
//		importBuilder.setDateLastValueColumn("log_id");//手动指定日期增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("logdb2db_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//增量配置结束

		  
        importBuilder.setGeoipDatabase("C:/workdir/geolite2/GeoLite2-City.mmdb");
        importBuilder.setGeoipAsnDatabase("C:/workdir/geolite2/GeoLite2-ASN.mmdb");
        importBuilder.setGeoip2regionDatabase("C:/workdir/geolite2/ip2region_v4.xdb;C:/workdir/geolite2/ip2region_v6.xdb");

		/**"
		 * 重新设置数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				 

				context.addFieldValue("author","duoduo");
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");
				context.addFieldValue("collecttime",new Date());//

				context.addIgnoreFieldMapping("subtitle");
				 
				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
				Date optime = context.getDateValue("LOG_OPERTIME",dateFormat);
				context.addFieldValue("logOpertime",optime);
				context.addFieldValue("collecttime",new Date());
 
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

		importBuilder 				.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
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
		 * 执行数据库表数据导入es操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作

		logger.info("come to end.");


	}

}
