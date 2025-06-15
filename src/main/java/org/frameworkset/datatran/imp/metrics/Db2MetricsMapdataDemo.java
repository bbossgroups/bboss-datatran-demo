package org.frameworkset.datatran.imp.metrics;
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

import com.frameworkset.util.BaseSimpleStringUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.boot.ElasticsearchBootResult;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.plugin.metrics.output.*;
import org.frameworkset.util.beans.ObjectHolder;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: 基于数字类型db-es增量同步及指标统计计算案例，如需调试功能，直接运行main方法即可
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2MetricsMapdataDemo {
	private static Logger logger = LoggerFactory.getLogger(Db2MetricsMapdataDemo.class);
	public static void main(String args[]){
		Db2MetricsMapdataDemo db2EleasticsearchDemo = new Db2MetricsMapdataDemo();
		//从配置文件application.properties中获取参数值
		boolean dropIndice = PropertiesUtil.getPropertiesContainer("application.properties").getBooleanSystemEnvProperty("dropIndice",true);
//		dbdemo.fullImportData(  dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
		db2EleasticsearchDemo.scheduleTimestampImportData(dropIndice);
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
		//在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作


/**
 * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
 */
        ObjectHolder<BulkProcessor> objectHolder = new ObjectHolder<BulkProcessor>();
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {
                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        //bulkprocessor和Elasticsearch输出插件共用Elasticsearch数据源，因此额外进行数据源初始化定义
                        Map properties = new HashMap();

//metricsES为的Elasitcsearch数据源名称
                        properties.put("elasticsearch.serverNames","metricsES");

                        /**
                         * metricsES数据源配置，每个配置项可以加metricsES.前缀
                         */


                        properties.put("metricsES.elasticsearch.rest.hostNames","192.168.137.1:9200");
                        properties.put("metricsES.elasticsearch.showTemplate","true");
                        properties.put("metricsES.elasticUser","elastic");
                        properties.put("metricsES.elasticPassword","changeme");
                        properties.put("metricsES.elasticsearch.failAllContinue","true");
                        properties.put("metricsES.http.timeoutSocket","60000");
                        properties.put("metricsES.http.timeoutConnection","40000");
                        properties.put("metricsES.http.connectionRequestTimeout","70000");
                        properties.put("metricsES.http.maxTotal","200");
                        properties.put("metricsES.http.defaultMaxPerRoute","100");
                        ResourceStartResult resourceStartResult = ElasticSearchBoot.boot(properties);

                        return resourceStartResult;
                    }
                });
            }

            @Override
            public void afterStartAction(ImportContext importContext) {

                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil("metricsES").dropIndice("vops-loginmodulemetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginmodulemetrics failed:",e);
                }
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil("metricsES").dropIndice("vops-loginusermetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginusermetrics failed:",e);
                }
                /**
                 * 构建一个指标数据写入Elasticsearch批处理器
                 */
                BulkProcessorBuilder bulkProcessorBuilder = new BulkProcessorBuilder();
                bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

                        .setBulkSizes(200)//按批处理数据记录数
                        .setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

                        .setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
                        .setWorkThreads(10)//bulk处理工作线程数
                        .setWorkThreadQueue(50)//bulk处理工作线程池缓冲队列大小
                        .setBulkProcessorName("detail_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
                        .setBulkRejectMessage("detail bulkprocessor")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
                        .setElasticsearch("metricsES")//指定明细Elasticsearch集群数据源名称，bboss可以支持多数据源
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
                objectHolder.setObject(bulkProcessor);


            }
        });
        //作业结束后销毁初始化阶段自定义的http数据源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {

                objectHolder.getObject().shutDown();//作业结束时关闭批处理器
//销毁初始化阶段自定义的数据源
                importContext.destroyResources(new ResourceEnd() {
                    @Override
                    public void endResource(ResourceStartResult resourceStartResult) {


                        if (resourceStartResult instanceof ElasticsearchBootResult) {
                            ElasticsearchBootResult elasticsearchBootResult = (ElasticsearchBootResult) resourceStartResult;
                            Map<String, Object> initedElasticsearch = elasticsearchBootResult.getResourceStartResult();
                            if (BaseSimpleStringUtil.isNotEmpty(initedElasticsearch)) {
                                ElasticSearchHelper.stopElasticsearchs(initedElasticsearch);
                            }
                        }
                    }
                });
            }
        });

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
				.setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") 
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(true)
				.setDbInitSize(5)
				.setDbMinIdleSize(5)
				.setDbMaxSize(10)
				.setShowSql(true);//是否使用连接池;
		importBuilder.setInputConfig(dbInputConfig);


		ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_KeyTimeMetircs){
			@Override
			public void builderMetrics(){
                //自定义MapData，只能设置一个BuildMapData
                setBuildMapData(metricsData -> {
                    BuildMapDataContext buildMapDataContext = metricsData.getBuildMapDataContext();
                    ETLMapData mapData = new ETLMapData(){
                        /**
                         * 根据指标标识，获取指标的时间统计维度字段，默认返回dataTime字段值，不同的指标需要指定不同的时间维度统计字段
                         * 分析处理作业可以覆盖本方法，自定义获取时间维度字段值
                         * @param metricsKey
                         * @return
                         */
                        public Date metricsDataTime(MetricKey metricsKey) {
//						if(metricsKey.equals("xxxx") ) {
//							Date time = (Date)data.get("collectime");
//							return time;
//						}
                            return getDataTime();
                        }

                    };
                   
                    return mapData;
                });
                //可以添加多个指标构建器
				//指标1 按操作模块统计模块操作次数
				addMetricBuilder(new MetricBuilder() {
					@Override
					public MetricKey buildMetricKey(MapData mapData){ //生成指标key
                        CommonRecord data = (CommonRecord) mapData.getData();
                        String operModule = (String) data.getData("operModule");
                        if(operModule == null || operModule.equals("")){
                            operModule = "未知模块";
                        }
						return new MetricKey(operModule);
					}
					@Override
					public KeyMetricBuilder metricBuilder(){
						return new KeyMetricBuilder() {
							@Override
							public KeyMetric build() {//生成指标对象
								return new LoginModuleMetric();
							}
						};
					}
				});

				//指标2 按照用户统计操作次数
				addMetricBuilder(new MetricBuilder() {
					@Override
					public MetricKey buildMetricKey(MapData mapData){
                        CommonRecord data = (CommonRecord) mapData.getData();
                        String logUser = (String) data.getData("logOperuser");//
                        if(logUser == null || logUser.equals("")){
                            logUser = "未知用户";
                        }
						return new MetricKey(logUser);
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
				setTimeWindows(60 );//统计时间窗口
                this.setTimeWindowType(MetricsConfig.TIME_WINDOW_TYPE_MINUTE);//统计时间窗口类型
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
						esData.put("hour", testKeyMetric.getDayHour());
						esData.put("minute", testKeyMetric.getMinute());
						esData.put("day", testKeyMetric.getDay());
						esData.put("metric", testKeyMetric.getMetric());
						esData.put("operModule", testKeyMetric.getOperModule());
						esData.put("count", testKeyMetric.getCount());
                        objectHolder.getObject().insertData("vops-loginmodulemetrics", esData);
					}
					else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
						Map esData = new HashMap();
						esData.put("dataTime", testKeyMetric.getDataTime());
						esData.put("hour", testKeyMetric.getDayHour());
						esData.put("minute", testKeyMetric.getMinute());
						esData.put("day", testKeyMetric.getDay());
						esData.put("metric", testKeyMetric.getMetric());
						esData.put("logUser", testKeyMetric.getLogUser());
						esData.put("count", testKeyMetric.getCount());
                        objectHolder.getObject().insertData("vops-loginusermetrics", esData);
					}

				});

			}
		};
        keyMetrics.setDataTimeField("logOpertime");
        // 直接实现map和persistent方法，定义一个ETLMetrics
        ETLMetrics keyMetrics1 = new ETLMetrics(Metrics.MetricsType_KeyTimeMetircs){
            @Override
            public void map(MapData mapData) {
                CommonRecord data = (CommonRecord) mapData.getData();
                //可以添加多个指标

                //指标1 按操作模块统计模块操作次数
                String operModule = (String) data.getData("operModule");
                if(operModule == null || operModule.equals("")){
                    operModule = "未知模块";
                }
                String metricKey = operModule;
                metric(metricKey, mapData, new KeyMetricBuilder() {
                    @Override
                    public KeyMetric build() {
                        return new LoginModuleMetric();
                    }

                });

                //指标2 按照用户统计操作次数
                String logUser = (String) data.getData("logOperuser");
                metricKey = logUser;
                metric(metricKey, mapData, new KeyMetricBuilder() {
                    @Override
                    public KeyMetric build() {
                        return new LoginUserMetric();
                    }

                });


            }

            @Override
            public void persistent(Collection< KeyMetric> metrics) {
                metrics.forEach(keyMetric->{
                    if(keyMetric instanceof LoginModuleMetric) {
                        LoginModuleMetric testKeyMetric = (LoginModuleMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("operModule", testKeyMetric.getOperModule());
                        esData.put("count", testKeyMetric.getCount());
                        objectHolder.getObject().insertData("vops-loginmodulemetrics", esData);
                    }
                    else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("logUser", testKeyMetric.getLogUser());
                        esData.put("count", testKeyMetric.getCount());
                        objectHolder.getObject().insertData("vops-loginusermetrics", esData);
                    }

                });

            }
        };
        //如果要自定义创建MapData,设置BuildMapData即可
        keyMetrics1.setBuildMapData(metricsData -> {
            BuildMapDataContext buildMapDataContext = metricsData.getBuildMapDataContext();
            ETLMapData mapData = new ETLMapData(){
                /**
                 * 根据指标标识，获取指标的时间统计维度字段，默认返回dataTime字段值，不同的指标需要指定不同的时间维度统计字段
                 * 分析处理作业可以覆盖本方法，自定义获取时间维度字段值
                 * @param metricsKey
                 * @return
                 */
                public Date metricsDataTime(MetricKey metricsKey) {
//						if(metricsKey.equals("xxxx") ) {
//							Date time = (Date)data.get("collectime");
//							return time;
//						}
                    return getDataTime();
                }

            };
            
            return mapData;
        });
        keyMetrics1.setDataTimeField("logOpertime");
        // key metrics中包含两个segment(S0,S1)
        keyMetrics1.setSegmentBoundSize(5000000);
        keyMetrics1.setTimeWindows(10);
        keyMetrics1.init();
        MetricsOutputConfig metricsOutputConfig = new MetricsOutputConfig();

        //添加2个指标计算器
        metricsOutputConfig.addMetrics(keyMetrics);
        metricsOutputConfig.addMetrics(keyMetrics1);

		importBuilder.setOutputConfig(metricsOutputConfig);


		importBuilder
//
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次
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
		});
//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
//		importBuilder.setStatusDbname("test");//设置增量状态数据源名称
		importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
//		setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("db2metricsmapdata_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logstable");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型

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
		 * 构建和执行数据库表数据导入es和指标统计作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();


	}


}
