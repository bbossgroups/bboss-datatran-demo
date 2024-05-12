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

import com.frameworkset.common.poolman.BatchHandler;
import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.bulk.*;
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
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.metrics.output.BuildMapData;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsData;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * <p>Description: 基于数字类型db-db增量数据采集并指标统计计算案例，如需调试功能，直接运行main方法即可
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2MetricsMapdata2DBDemo {
	private static Logger logger = LoggerFactory.getLogger(Db2MetricsMapdata2DBDemo.class);
	public static void main(String args[]){
		Db2MetricsMapdata2DBDemo db2EleasticsearchDemo = new Db2MetricsMapdata2DBDemo();
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
        ObjectHolder<CommonBulkProcessor> objectHolder = new ObjectHolder<CommonBulkProcessor>();
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {

            }

            @Override
            public void afterStartAction(ImportContext importContext) {



                int bulkSize = 150;
                int workThreads = 5;
                int workThreadQueue = 100;
                //定义BulkProcessor批处理组件构建器
                CommonBulkProcessorBuilder bulkProcessorBuilder = new CommonBulkProcessorBuilder();
                bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

                        .setBulkSizes(bulkSize)//按批处理数据记录数
                        .setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

                        .setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
                        .setWorkThreads(workThreads)//bulk处理工作线程数
                        .setWorkThreadQueue(workThreadQueue)//bulk处理工作线程池缓冲队列大小
                        .setBulkProcessorName("db_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
                        .setBulkRejectMessage("db bulkprocessor ")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
                        .addBulkInterceptor(new CommonBulkInterceptor() {// 添加异步处理结果回调函数
                            /**
                             * 执行前回调方法
                             * @param bulkCommand
                             */
                            public void beforeBulk(CommonBulkCommand bulkCommand) {

                            }

                            /**
                             * 执行成功回调方法
                             * @param bulkCommand
                             * @param result
                             */
                            public void afterBulk(CommonBulkCommand bulkCommand, BulkResult result) {
                                if(logger.isDebugEnabled()){
//                           logger.debug(result.getResult());
                                }
                            }

                            /**
                             * 执行异常回调方法
                             * @param bulkCommand
                             * @param exception
                             */
                            public void exceptionBulk(CommonBulkCommand bulkCommand, Throwable exception) {
                                if(logger.isErrorEnabled()){
                                    logger.error("exceptionBulk",exception);
                                }
                            }

                            /**
                             * 执行过程中部分数据有问题回调方法
                             * @param bulkCommand
                             * @param result
                             */
                            public void errorBulk(CommonBulkCommand bulkCommand, BulkResult result) {
                                if(logger.isWarnEnabled()){
//                            logger.warn(result);
                                }
                            }
                        })//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
                        /**
                         * 设置执行数据批处理接口，实现对数据的异步批处理功能逻辑
                         */
                        .setBulkAction(new BulkAction() {
                            public BulkResult execute(CommonBulkCommand command) {
                                List<CommonBulkData> bulkDataList = command.getBatchBulkDatas();//拿出要进行批处理操作的数据
                                List<Map> logUserMetrics = new ArrayList<>();
                                List<Map> logModuleMetrics = new ArrayList<>();
                                for(int i = 0; i < bulkDataList.size(); i ++){
                                    CommonBulkData commonBulkData = bulkDataList.get(i);
                                    Map data = (Map)commonBulkData.getData();
                                    if(data.get("operModule") != null){
                                        logModuleMetrics.add(data);
                                    }
                                    else{
                                        logUserMetrics.add(data);
                                    }
                                    /**
                                     * 可以根据操作类型，对数据进行相应处理
                                     */
//                            if(commonBulkData.getType() == CommonBulkData.INSERT) 新增记录
//                            if(commonBulkData.getType() == CommonBulkData.UPDATE) 修改记录
//                            if(commonBulkData.getType() == CommonBulkData.DELETE) 删除记录

                                }
                                BulkResult bulkResult = new BulkResult();//构建批处理操作结果对象
                                try {
                                    //调用数据库dao executor，将数据批量写入数据库，对应的sql语句addPositionUrl在xml配置文件dbbulktest.xml中定义


                                    if(logUserMetrics.size() > 0) {
                                        //直接使用dbinput插件配置的数据源test，可以参考文档自定义数据库数据源
                                        SQLExecutor.executeBatch("test","insert into logusermetrics(dataTime,hour,minute,day,metric,logUser,count) values(?,?,?,?,?,?,?)", logUserMetrics, 150, new BatchHandler<Map>() {
                                            public void handler(PreparedStatement stmt, Map record, int i) throws SQLException {
                                                stmt.setTimestamp(1, new java.sql.Timestamp(((Date)record.get("dataTime")).getTime()));
                                                stmt.setString(2, (String)record.get("hour"));
                                                stmt.setString(3, (String)record.get("minute"));
                                                stmt.setString(4, (String)record.get("day"));
                                                stmt.setString(5, (String)record.get("metric"));
                                                stmt.setString(6, (String)record.get("logUser"));
                                                stmt.setLong(6, (Long)record.get("count"));
                                            }
                                        });
                                    }

                                    if(logModuleMetrics.size() > 0) {
                                        //直接使用dbinput插件配置的数据源test
                                        SQLExecutor.executeBatch("test","insert into logmodulermetrics(dataTime,hour,minute,day,metric,operModule,count) values(?,?,?,?,?,?,?)", logModuleMetrics, 150, new BatchHandler<Map>() {
                                            public void handler(PreparedStatement stmt, Map record, int i) throws SQLException {
                                                stmt.setTimestamp(1, new java.sql.Timestamp(((Date)record.get("dataTime")).getTime()));
                                                stmt.setString(2, (String)record.get("hour"));
                                                stmt.setString(3, (String)record.get("minute"));
                                                stmt.setString(4, (String)record.get("day"));
                                                stmt.setString(5, (String)record.get("metric"));
                                                stmt.setString(6, (String)record.get("operModule"));
                                                stmt.setLong(6, (Long)record.get("count"));
                                            }
                                        });
                                    }

                                }
                                catch (Exception e){
                                    //如果执行出错，则将错误信息设置到结果中
                                    logger.error("",e);
                                    bulkResult.setError(true);
                                    bulkResult.setErrorInfo(e.getMessage());
                                }
                                return bulkResult;
                            }
                        })
                ;
                /**
                 * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
                 */
                final CommonBulkProcessor bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
                objectHolder.setObject(bulkProcessor);


            }
        });
        //作业结束后销毁初始化阶段自定义的http数据源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {

                objectHolder.getObject().shutDown();//作业结束时关闭批处理器

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
				.setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(true)
				.setDbInitSize(5)
				.setDbMinIdleSize(5)
				.setDbMaxSize(10)
				.setShowSql(true);//是否使用连接池;
		importBuilder.setInputConfig(dbInputConfig);





        // 直接实现map和persistent方法，定义一个ETLMetrics
        ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_KeyTimeMetircs){
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
                        objectHolder.getObject().insertData( esData);
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
                        objectHolder.getObject().insertData( esData);
                    }

                });

            }
        };
        //如果要自定义创建MapData,设置BuildMapData即可
        keyMetrics.setBuildMapData(new BuildMapData() {
            @Override
            public MapData buildMapData(MetricsData metricsData) {
                BuildMapDataContext buildMapDataContext = metricsData.getBuildMapDataContext();
                MapData mapData = new MapData(){
                    /**
                     * 根据指标标识，获取指标的时间统计维度字段，默认返回dataTime字段值，不同的指标需要指定不同的时间维度统计字段
                     * 分析处理作业可以覆盖本方法，自定义获取时间维度字段值
                     * @param metricsKey
                     * @return
                     */
                    public Date metricsDataTime(String metricsKey) {
//						if(metricsKey.equals("xxxx") ) {
//							Date time = (Date)data.get("collectime");
//							return time;
//						}
                        return getDataTime();
                    }

                };
                Date dateTime = (Date) metricsData.getCommonRecord().getData("logOpertime");
                mapData.setDataTime(dateTime);//默认按照操作时间作为指标时间维度字段，上面复写了metricsDataTime方法，可以根据指标key指定不同的时间维度值
                mapData.setData(metricsData.getCommonRecord());
                mapData.setDayFormat(buildMapDataContext.getDayFormat());
                mapData.setHourFormat(buildMapDataContext.getHourFormat());
                mapData.setMinuteFormat(buildMapDataContext.getMinuteFormat());
                mapData.setYearFormat(buildMapDataContext.getYearFormat());
                mapData.setMonthFormat(buildMapDataContext.getMonthFormat());
                mapData.setWeekFormat(buildMapDataContext.getWeekFormat());
                return mapData;
            }
        });
        // key metrics中包含两个segment(S0,S1)
        keyMetrics.setSegmentBoundSize(5000000);
        keyMetrics.setTimeWindows(10);
        keyMetrics.init();
        MetricsOutputConfig metricsOutputConfig = new MetricsOutputConfig();

        //添加2个指标计算器
        metricsOutputConfig.addMetrics(keyMetrics);

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
