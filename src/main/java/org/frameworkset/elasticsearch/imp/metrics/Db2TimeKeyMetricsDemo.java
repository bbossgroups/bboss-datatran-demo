package org.frameworkset.elasticsearch.imp.metrics;
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
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: 全量同步KeyMetrics指标统计计算案例，如需调试功能，直接运行main方法即可
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2TimeKeyMetricsDemo {
	private static Logger logger = LoggerFactory.getLogger(Db2TimeKeyMetricsDemo.class);
	public static void main(String args[]){
		Db2TimeKeyMetricsDemo db2EleasticsearchDemo = new Db2TimeKeyMetricsDemo();
		//从配置文件application.properties中获取参数值
		boolean dropIndice = PropertiesUtil.getPropertiesContainer("application.properties").getBooleanSystemEnvProperty("dropIndice",true);
//		dbdemo.fullImportData(  dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
		db2EleasticsearchDemo.doImportData(dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
//		args[1].charAt(0) == args[2].charAt(0);
	}

	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 * 从配置文件application.properties中获取参数值方法
	 * boolean dropIndice = PropertiesUtil.getPropertiesContainer().getBooleanSystemEnvProperty("dropIndice",true);
	 * int threadCount = PropertiesUtil.getPropertiesContainer().getIntSystemEnvProperty("log.threadCount",2);
	 */
	public void doImportData(boolean dropIndice){

		ImportBuilder importBuilder = new ImportBuilder() ;
		//在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作


        importBuilder.setImportStartAction(new ImportStartAction() {
            /**
             * 初始化之前执行的处理操作，比如后续初始化操作、数据处理过程中依赖的资源初始化
             * @param importContext
             */
            @Override
            public void startAction(ImportContext importContext) {
            }

            /**
             * 所有初始化操作完成后，导出数据之前执行的操作
             * @param importContext
             */
            @Override
            public void afterStartAction(ImportContext importContext) {
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil().dropIndice("vops-loginmodulemetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginmodulemetrics failed:",e);
                }
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil().dropIndice("vops-loginusermetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginusermetrics failed:",e);
                }

            }
        });

		DBInputConfig dbInputConfig = new DBInputConfig();
		//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
		// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
		// select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
		// 需要设置setLastValueColumn信息log_id，
		// 通过setLastValueType方法告诉工具增量字段的类型，默认是数字类型

//		importBuilder.setSql("select * from td_sm_log where LOG_OPERTIME > #[LOG_OPERTIME]");
		dbInputConfig.setSql("select * from td_sm_log")
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


        //bulkprocessor和Elasticsearch输出插件共用Elasticsearch数据源，因此额外进行数据源初始化定义
        Map properties = new HashMap();

//default为默认的Elasitcsearch数据源名称
        properties.put("elasticsearch.serverNames","default");

        /**
         * 默认的default数据源配置，每个配置项可以加default.前缀，也可以不加
         */


        properties.put("default.elasticsearch.rest.hostNames","192.168.137.1:9200");
        properties.put("default.elasticsearch.showTemplate","true");
        properties.put("default.elasticUser","elastic");
        properties.put("default.elasticPassword","changeme");
        properties.put("default.elasticsearch.failAllContinue","true");
        properties.put("default.http.timeoutSocket","60000");
        properties.put("default.http.timeoutConnection","40000");
        properties.put("default.http.connectionRequestTimeout","70000");
        properties.put("default.http.maxTotal","200");
        properties.put("default.http.defaultMaxPerRoute","100");
        ElasticSearchBoot.boot(properties);

		BulkProcessorBuilder bulkProcessorBuilder = new BulkProcessorBuilder();
		bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

				.setBulkSizes(200)//按批处理数据记录数
				.setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

				.setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
				.setWorkThreads(10)//bulk处理工作线程数
				.setWorkThreadQueue(50)//bulk处理工作线程池缓冲队列大小
				.setBulkProcessorName("detail_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
				.setBulkRejectMessage("detail bulkprocessor")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
				.setElasticsearch("default")//指定明细Elasticsearch集群数据源名称，bboss可以支持多数据源
				.setFilterPath(BulkConfig.ERROR_FILTER_PATH)
				.addBulkInterceptor(new BulkInterceptor() {
					public void beforeBulk(BulkCommand bulkCommand) {

					}

					public void afterBulk(BulkCommand bulkCommand, String result) {
						if(logger.isDebugEnabled()){
							logger.debug(result);
						}
					}

					public void exceptionBulk(BulkCommand bulkCommand, Throwable exception) {
						if(logger.isErrorEnabled()){
							logger.error("exceptionBulk",exception);
						}
					}
					public void errorBulk(BulkCommand bulkCommand, String result) {
						if(logger.isWarnEnabled()){
							logger.warn(result);
						}
					}
				})//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
		;
		/**
		 * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
		 */
		BulkProcessor bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
		ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_TimeKeyMetircs){
//        ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_KeyTimeMetircs){
			@Override
			public void builderMetrics(){
				//指标1 按操作模块统计模块操作次数
				addMetricBuilder(new MetricBuilder() {
					@Override
					public String buildMetricKey(MapData mapData){
                        CommonRecord data = (CommonRecord) mapData.getData();
                        logger.info(SimpleStringUtil.object2json(data.getDatas()));
                        String operModule = (String) data.getData("operModule");
                        if(operModule == null || operModule.equals("")){
                            operModule = "未知模块";
                        }
						return operModule;
					}
					@Override
					public KeyMetricBuilder metricBuilder(){
						return new KeyMetricBuilder() {
							@Override
							public KeyMetric build() {
								return new LoginModuleMetric();
							}
						};
					}
				});

				//指标2 按照用户统计操作次数
				addMetricBuilder(new MetricBuilder() {
					@Override
					public String buildMetricKey(MapData mapData){
                        CommonRecord data = (CommonRecord) mapData.getData();
                        String logUser = (String) data.getData("logOperuser");//
                        if(logUser == null || logUser.equals("")){
                            logUser = "未知用户";
                        }
						return logUser;
					}
					@Override
					public KeyMetricBuilder metricBuilder(){
						return new KeyMetricBuilder() {
							@Override
							public KeyMetric build() {
								return new LoginUserMetric();
							}
						};
					}
				});
				// key metrics中包含两个segment(S0,S1)
				setSegmentBoundSize(5000000);
                setTimeWindowType(MetricsConfig.TIME_WINDOW_TYPE_DAY);
			}

            /**
             * 存储指标计算结果
             * @param metrics
             */
			@Override
			public void persistent(Collection< KeyMetric> metrics) {
				metrics.forEach(keyMetric->{
					if(keyMetric instanceof LoginModuleMetric) {
                        LoginModuleMetric testKeyMetric = (LoginModuleMetric) keyMetric;
						Map esData = new HashMap();
						esData.put("dataTime", testKeyMetric.getDataTime());
                        esData.put("year", testKeyMetric.getYear());
                        esData.put("month", testKeyMetric.getMonth());
                        esData.put("week", testKeyMetric.getWeek());
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("timeMetricKey", testKeyMetric.getMetricTimeKey());
						esData.put("metric", testKeyMetric.getMetric());
						esData.put("operModule", testKeyMetric.getOperModule());
						esData.put("count", testKeyMetric.getCount());
						bulkProcessor.insertData("vops-loginmodulekeymetrics", esData);
					}
					else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
						Map esData = new HashMap();
						esData.put("dataTime", testKeyMetric.getDataTime());
                        esData.put("year", testKeyMetric.getYear());
                        esData.put("month", testKeyMetric.getMonth());
                        esData.put("week", testKeyMetric.getWeek());
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
						esData.put("metric", testKeyMetric.getMetric());
						esData.put("logUser", testKeyMetric.getLogUser());
                        esData.put("timeMetricKey", testKeyMetric.getMetricTimeKey());
						esData.put("count", testKeyMetric.getCount());
						bulkProcessor.insertData("vops-loginuserkeymetrics", esData);
					}

				});

			}
		};
        //作业结束后销毁初始化阶段自定义的http数据源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {

                bulkProcessor.shutDown();

            }
        });
        MetricsOutputConfig metricsOutputConfig = new MetricsOutputConfig();

        metricsOutputConfig.setDataTimeField("logOpertime");
        metricsOutputConfig.addMetrics(keyMetrics);

		importBuilder.setOutputConfig(metricsOutputConfig);


		importBuilder
//
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理



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


		});


		/**
		 * 构建和执行数据库表数据导入es和指标统计作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();


	}


}
