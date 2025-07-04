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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.TranMeta;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.util.Date;

/**
 * <p>Description: 一个具备状态记录的一次性导入任务，具备故障检测点功能，如果在导入过程中出现问题，重启作业会从故障点开始同步数据，基于数字类型db-es一次性同步案例，同步处理程序，如需调试同步功能，直接运行main方法即可
 *   指定任务执行日期
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2EleasticsearchOnceIncrScheduleDateDemo {
	private static Logger logger = LoggerFactory.getLogger(Db2EleasticsearchOnceIncrScheduleDateDemo.class);
	public static void main(String args[]){
		Db2EleasticsearchOnceIncrScheduleDateDemo db2EleasticsearchDemo = new Db2EleasticsearchOnceIncrScheduleDateDemo();
		//从配置文件application.properties中获取参数值
		boolean dropIndice = PropertiesUtil.getPropertiesContainer("application.properties").getBooleanSystemEnvProperty("dropIndice",true);
//		dbdemo.fullImportData(  dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
		db2EleasticsearchDemo.scheduleTimestampImportData(dropIndice);

		SQLiteConfig config = new SQLiteConfig();
//		dbdemo.scheduleImportData(dropIndice);
//		args[1].charAt(0) == args[2].charAt(0);
	}

	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 * 从配置文件application.properties中获取参数值方法
	 * boolean dropIndice = PropertiesUtil.getPropertiesContainer().getBooleanSystemEnvProperty("dropIndice",true);
	 * int threadCount = PropertiesUtil.getPropertiesContainer().getIntSystemEnvProperty("log.threadCount",2);
	 */
	public void scheduleTimestampImportData(boolean dropIndice){

		ImportBuilder importBuilder = new ImportBuilder() ;
		DBInputConfig dbInputConfig = new DBInputConfig();
		//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
		// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
		// select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
		// 需要设置setLastValueColumn信息log_id，
		// 通过setLastValueType方法告诉工具增量字段的类型，默认是数字类型

//		importBuilder.setSql("select * from td_sm_log where LOG_OPERTIME > #[LOG_OPERTIME]");
		dbInputConfig.setSql("select * from td_sm_log where log_id > #[log_id]")
				.setDbName("test")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
				.setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true") 
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
                .setJdbcFetchSize(-2147483648)//使用mysql 缺省流处理机制
				.setUsePool(false)//其实可以不用连接池

				.setShowSql(true);//是否使用连接池;
		importBuilder.setInputConfig(dbInputConfig);

        importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
        importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
//		setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
        importBuilder.setStatusDbname("testStatus");//指定增量状态数据源名称
		importBuilder.setLastValueStorePath("Db2EleasticsearchOnceIncrScheduleDateDemo");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        importBuilder.setLastValueStoreTableName("logstable1");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
        importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
//


//		importBuilder.addFieldMapping("LOG_CONTENT","message");
//		importBuilder.addIgnoreFieldMapping("remark1");
//		importBuilder.setSql("select * from td_sm_log ");
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
		elasticsearchOutputConfig
				.addTargetElasticsearch("elasticsearch.serverNames", "default")
				.addElasticsearchProperty("default.elasticsearch.rest.hostNames", "192.168.137.1:9200")
				.addElasticsearchProperty("default.elasticsearch.showTemplate", "true")
				.addElasticsearchProperty("default.elasticUser", "elastic")
				.addElasticsearchProperty("default.elasticPassword", "changeme")
				.addElasticsearchProperty("default.elasticsearch.failAllContinue", "true")
				.addElasticsearchProperty("default.http.timeoutSocket", "60000")
				.addElasticsearchProperty("default.http.timeoutConnection", "40000")
				.addElasticsearchProperty("default.http.connectionRequestTimeout", "70000")
				.addElasticsearchProperty("default.http.maxTotal", "200")
				.addElasticsearchProperty("default.http.defaultMaxPerRoute", "100")
				.setIndex("dbdemo")
				.setEsIdField("log_id")//设置文档主键，不设置，则自动产生文档id
				.setDebugResponse(false)//设置是否将每次处理的reponse打印到日志文件中，默认false
				.setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false

		importBuilder.setOutputConfig(elasticsearchOutputConfig);

		/**
		 * 设置IP地址信息库
		 */
		importBuilder.setGeoipDatabase("d:/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("d:/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("d:/geolite2/ip2region.db");

		importBuilder
//
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
				.setUseLowcase(true)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

		//指定任务开始执行时间：日期，1分钟后开始
		importBuilder.setScheduleDate(TimeUtil.addDateMinitues(new Date(),1));
//				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行


		//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//
		/**
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
//				Date date = context.getDateValue("LOG_OPERTIME");
				context.addFieldValue("collecttime",new Date());
				IpInfo ipInfo = context.getIpInfoByIp("219.133.80.136");
				if(ipInfo != null)
					context.addFieldValue("ipInfo", SimpleStringUtil.object2json(ipInfo));
//				HttpRequestProxy.sendJsonBody("datatran",ipInfo,"/httpservice/sendData.api");

				TranMeta tranMeta = context.getMetaData();
				logger.info("tranMeta {}",tranMeta);
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

		importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
			@Override
			public void success(TaskCommand<String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
				logger.debug(result);
			}

			@Override
			public void error(TaskCommand<String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
				logger.debug(result);
			}

			@Override
			public void exception(TaskCommand<String> taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.debug(taskMetrics.toString());
			}


		});


		/**
		 * 执行数据库表数据导入es操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作


	}


}
