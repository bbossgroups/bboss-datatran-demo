package org.frameworkset.elasticsearch.imp;
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

import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.DBStartResult;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.spi.remote.http.HttpResourceStartResult;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: 基于数字类型db-es一次性同步案例，同步处理程序，如需调试同步功能，直接运行main方法即可
 *   指定任务执行日期
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2EleasticsearchOnceScheduleDateDemo {
	private static Logger logger = LoggerFactory.getLogger(Db2EleasticsearchOnceScheduleDateDemo.class);
	public static void main(String args[]){
		Db2EleasticsearchOnceScheduleDateDemo db2EleasticsearchDemo = new Db2EleasticsearchOnceScheduleDateDemo();
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
		dbInputConfig.setSql("select * from td_sm_log ")
				.setDbName("test")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
				.setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(true)
				.setDbInitSize(5)
				.setDbMinIdleSize(5)
				.setDbMaxSize(10)
				.setShowSql(true);//是否使用连接池;
		importBuilder.setInputConfig(dbInputConfig);

		//在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作

		//在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作

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
						Map<String,Object> configs = new LinkedHashMap<>();
						configs.put("http.poolNames","datatran");
						configs.put("datatran.http.health","/health");
						configs.put("datatran.http.hosts","192.168.137.1:808");
						configs.put("datatran.http.timeoutConnection","5000");
						configs.put("datatran.http.timeoutSocket","50000");
						configs.put("datatran.http.connectionRequestTimeout","50000");
						configs.put("datatran.http.maxTotal","200");
						configs.put("datatran.http.defaultMaxPerRoute","100");
						configs.put("datatran.http.failAllContinue","true");

						return HttpRequestProxy.startHttpPools(configs);
					}
				});

				importContext.addResourceStart(new ResourceStart() {
					@Override
					public ResourceStartResult startResource() {
						DBConf tempConf = new DBConf();
						tempConf.setPoolname("testStatus");
						tempConf.setDriver("com.mysql.cj.jdbc.Driver");
						tempConf.setJdbcurl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true");

						tempConf.setUsername("root");
						tempConf.setPassword("123456");
						tempConf.setValidationQuery("select 1");

						tempConf.setInitialConnections(5);
						tempConf.setMinimumSize(10);
						tempConf.setMaximumSize(10);
						tempConf.setUsepool(true);
						tempConf.setShowsql(true);
						tempConf.setJndiName("testStatus-jndi");

						//# 控制map中的列名采用小写，默认为大写
						tempConf.setColumnLableUpperCase(false);
						//启动数据源
						boolean result = SQLManager.startPool(tempConf);
						ResourceStartResult resourceStartResult = null;
						//记录启动的数据源信息，用户作业停止时释放数据源
						if(result){
							resourceStartResult = new DBStartResult();
							resourceStartResult.addResourceStartResult("testStatus");
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
				if(dropIndice) {
					try {
						//清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
						ElasticSearchHelper.getRestClientUtil().dropIndice("dbdemo");
					} catch (Exception e) {
						logger.error("Drop indice dbdemo failed:",e);
					}
				}
			}
		});

		//任务结束后销毁初始化阶段自定义的http数据源
		importBuilder.setImportEndAction(new ImportEndAction() {
			@Override
			public void endAction(ImportContext importContext, Exception e) {
				//销毁初始化阶段自定义的数据源
				importContext.destroyResources(new ResourceEnd() {
					@Override
					public void endResource(ResourceStartResult resourceStartResult) {
						if(resourceStartResult instanceof HttpResourceStartResult) {//作业停止时，释放http服务数据源
							HttpRequestProxy.stopHttpClients(resourceStartResult);
						}
						else if(resourceStartResult instanceof DBStartResult) { //作业停止时，释放db数据源
							DataTranPluginImpl.stopDatasources((DBStartResult) resourceStartResult);
						}
					}
				});
			}
		});


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
		//指定外部Elasticsearch数据源aaaa
//		elasticsearchOutputConfig
//				.setTargetElasticsearch("aaaa")
//				.setIndex("dbdemo")
//				.setEsIdField("log_id")//设置文档主键，不设置，则自动产生文档id
//				.setDebugResponse(false)//设置是否将每次处理的reponse打印到日志文件中，默认false
//				.setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
		/**
		 elasticsearchOutputConfig.setEsIdGenerator(new EsIdGenerator() {
		 //如果指定EsIdGenerator，则根据下面的方法生成文档id，
		 // 否则根据setEsIdField方法设置的字段值作为文档id，
		 // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id

		 @Override
		 public Object genId(Context context) throws Exception {
		 return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
		 }
		 });
		 */
//				.setIndexType("dbdemo") ;//es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType;
//				.setRefreshOption("refresh")//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
		/**
		 * es相关配置
		 */
//		elasticsearchOutputConfig.setTargetElasticsearch("default,test");//同步数据到两个es集群

		importBuilder.setOutputConfig(elasticsearchOutputConfig);

		/**
		 * 设置IP地址信息库
		 */
		importBuilder.setGeoipDatabase("E:/workspace/hnai/terminal/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("E:/workspace/hnai/terminal/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("E:/workspace/hnai/terminal/geolite2/ip2region.db");

		importBuilder
//
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
				.setUseLowcase(true)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

		//指定任务开始执行时间：日期，1分钟后开始
		importBuilder.setScheduleDate(TimeUtil.addDateMinitues(new Date(),1));
//				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行



		//定时任务配置结束
//
//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				System.out.println("preCall");
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall");
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				System.out.println("throwException");
			}
		}).addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				System.out.println("preCall 1");
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				System.out.println("throwException 1");
			}
		});
//		//设置任务执行拦截器结束，可以添加多个


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
				HttpRequestProxy.sendJsonBody("datatran",ipInfo,"/httpservice/sendData.api");

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
		importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回

		importBuilder.setExportResultHandler(new ExportResultHandler<String,String>() {
			@Override
			public void success(TaskCommand<String,String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
				logger.debug(result);
			}

			@Override
			public void error(TaskCommand<String,String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
				logger.debug(result);
			}

			@Override
			public void exception(TaskCommand<String,String> taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.debug(taskMetrics.toString());
			}

			@Override
			public int getMaxRetry() {
				return 0;
			}
		});


		/**
		 * 执行数据库表数据导入es操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作
		dataStream.destroy();//释放资源，一次性执行作业需要主动调用destroy方法释放资源，其他作业无效主动调用


	}


}
