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

import com.frameworkset.common.poolman.BatchHandler;
import com.frameworkset.common.poolman.ConfigSQLExecutor;
import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.DBStartResult;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.util.BaseSimpleStringUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.bulk.*;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.boot.ElasticsearchBootResult;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.DynamicParam;
import org.frameworkset.tran.config.DynamicParamContext;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.plugin.es.input.ElasticsearchInputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.frameworkset.util.beans.ObjectHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * <p>Description: 基于数字类型db-es增量同步及指标统计计算案例，如需调试功能，直接运行main方法即可
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Elasticsearch2EleasticsearchMetricsDemo {
	private static Logger logger = LoggerFactory.getLogger(Elasticsearch2EleasticsearchMetricsDemo.class);
	public static void main(String args[]){
		Elasticsearch2EleasticsearchMetricsDemo db2EleasticsearchDemo = new Elasticsearch2EleasticsearchMetricsDemo();
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
                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        DBConf tempConf = new DBConf();
                        tempConf.setPoolname("clickhouse");
                        tempConf.setDriver("com.mysql.cj.jdbc.Driver");
                        tempConf.setJdbcurl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true");

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

                importContext.addResourceStart(new ResourceStart() {
                    
                    @Override
                    public ResourceStartResult startResource() {
                        //bulkprocessor和Elasticsearch输出插件共用Elasticsearch数据源，因此额外进行数据源初始化定义
                        Map properties = new HashMap();

                        properties.put("elasticsearch.serverNames","testES");

                        /**
                         * testES数据源配置，每个配置项可以加testES.前缀
                         */


                        properties.put("testES.elasticsearch.rest.hostNames","192.168.137.1:9200");
                        properties.put("testES.elasticsearch.showTemplate","true");
                        properties.put("testES.elasticUser","elastic");
                        properties.put("testES.elasticPassword","changeme");
                        properties.put("testES.elasticsearch.failAllContinue","true");
                        properties.put("testES.http.timeoutSocket","60000");
                        properties.put("testES.http.timeoutConnection","40000");
                        properties.put("testES.http.connectionRequestTimeout","70000");
                        properties.put("testES.http.maxTotal","200");
                        properties.put("testES.http.defaultMaxPerRoute","100");
                        return ElasticSearchBoot.boot(properties);
                    }
                });


            }

            @Override
            public void afterStartAction(ImportContext importContext) {
                int bulkSize = 150;
                int workThreads = 5;
                int workThreadQueue = 100;
                final ConfigSQLExecutor executor = new ConfigSQLExecutor("dbbulktest.xml");//加载sql配置文件，初始化一个db dao组件
//        DBInit.startDatasource(""); //初始化bboss数据源方法，参考文档：https://doc.bbossgroups.com/#/persistent/PersistenceLayer1
                //定义BulkProcessor批处理组件构建器
                CommonBulkProcessorBuilder bulkProcessorBuilder = new CommonBulkProcessorBuilder();
                bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

                        .setBulkSizes(bulkSize)//按批处理数据记录数
                        .setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次往bulk中添加记录的时间后，空闲了flushInterval毫秒后一直没有数据到来，且数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

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
                                //查看队列中追加的总记录数
                                logger.info("appendSize:"+bulkCommand.getAppendRecords());
                                //查看已经被处理成功的总记录数
                                logger.info("totalSize:"+bulkCommand.getTotalSize());
                                //查看处理失败的记录数
                                logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
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
                                //查看队列中追加的总记录数
                                logger.info("appendSize:"+bulkCommand.getAppendRecords());
                                //查看已经被处理成功的总记录数
                                logger.info("totalSize:"+bulkCommand.getTotalSize());
                                //查看处理失败的记录数
                                logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
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
                                //查看队列中追加的总记录数
                                logger.info("appendSize:"+bulkCommand.getAppendRecords());
                                //查看已经被处理成功的总记录数
                                logger.info("totalSize:"+bulkCommand.getTotalSize());
                                //查看处理失败的记录数
                                logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
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
                                //查看队列中追加的总记录数
                                logger.info("appendSize:"+bulkCommand.getAppendRecords());
                                //查看已经被处理成功的总记录数
                                logger.info("totalSize:"+bulkCommand.getTotalSize());
                                //查看处理失败的记录数
                                logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
                            }
                        })//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
                        /**
                         * 设置执行数据批处理接口，实现对数据的异步批处理功能逻辑
                         */
                        .setBulkAction(new BulkAction() {
                            public BulkResult execute(CommonBulkCommand command) {
                                List<CommonBulkData> bulkDataList = command.getBatchBulkDatas();//拿出要进行批处理操作的数据
                                List<LoginModuleMetric> positionUrls = new ArrayList<LoginModuleMetric>();
                                for(int i = 0; i < bulkDataList.size(); i ++){
                                    CommonBulkData commonBulkData = bulkDataList.get(i);
                                    /**
                                     * 可以根据操作类型，对数据进行相应处理
                                     */
//                            if(commonBulkData.getType() == CommonBulkData.INSERT) 新增记录
//                            if(commonBulkData.getType() == CommonBulkData.UPDATE) 修改记录
//                            if(commonBulkData.getType() == CommonBulkData.DELETE) 删除记录
                                    positionUrls.add((LoginModuleMetric)commonBulkData.getData());

                                }
                                BulkResult bulkResult = new BulkResult();//构建批处理操作结果对象
                                try {
                                    //调用数据库dao executor，将数据批量写入数据库，对应的sql语句addPositionUrl在xml配置文件dbbulktest.xml中定义
                                    executor.executeBatch("clickhouse","addPositionUrl", positionUrls, 150, new BatchHandler<LoginModuleMetric>() {
                                        public void handler(PreparedStatement stmt, LoginModuleMetric record, int i) throws SQLException {
                                            //id,positionUrl,positionName,createTime
                                            stmt.setString(1, record.getOperModule());
                                            stmt.setString(2, record.getDay());
                                            stmt.setString(3, record.getMetric());
                                            stmt.setTimestamp(4, new Timestamp(record.getDataTime().getTime()));
                                        }
                                    });

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

                CommonBulkProcessor commonBulkProcessor = bulkProcessorBuilder.build();
                objectHolder.setObject(commonBulkProcessor);
//                /**
//                 * 构建一个指标数据写入Elasticsearch批处理器
//                 */
//                
//                
//                
//                BulkProcessorBuilder bulkProcessorBuilder = new BulkProcessorBuilder();
//                bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止
//
//                        .setBulkSizes(200)//按批处理数据记录数
//                        .setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理
//
//                        .setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
//                        .setWorkThreads(10)//bulk处理工作线程数
//                        .setWorkThreadQueue(50)//bulk处理工作线程池缓冲队列大小
//                        .setBulkProcessorName("detail_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
//                        .setBulkRejectMessage("detail bulkprocessor")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
//                        .setElasticsearch("testES")//指定明细Elasticsearch集群数据源名称，bboss可以支持多数据源
//                        .setFilterPath(BulkConfig.ERROR_FILTER_PATH)
//                        .addBulkInterceptor(new BulkInterceptor() {
//                            public void beforeBulk(BulkCommand bulkCommand) {
//
//                            }
//
//                            public void afterBulk(BulkCommand bulkCommand, String result) {
//                                if(logger.isDebugEnabled()){
//                                    logger.debug(result);
//                                }
//                            }
//
//                            public void exceptionBulk(BulkCommand bulkCommand, Throwable exception) {
//                                if(logger.isErrorEnabled()){
//                                    logger.error("exceptionBulk",exception);
//                                }
//                            }
//                            public void errorBulk(BulkCommand bulkCommand, String result) {
//                                if(logger.isWarnEnabled()){
//                                    logger.warn(result);
//                                }
//                            }
//                        })//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
//                ;
//                /**
//                 * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
//                 */
//                BulkProcessor bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
//                objectHolder.setObject(bulkProcessor);

//                if(dropIndice) {
//                    ClientInterface clientInterface =  ElasticSearchHelper.getConfigRestClientUtil("testES","indice.xml");
//                    
//                    try {
//                        //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
//                        clientInterface.dropIndice("vops-loginmodulemetrics");
//                    } catch (Exception e) {
//                        logger.error("Drop indice  vops-loginmodulemetrics failed:",e);
//                    }
//                    try {
//                        //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
//                        clientInterface.dropIndice("vops-loginusermetrics");
//                    } catch (Exception e) {
//                        logger.error("Drop indice  vops-loginusermetrics failed:",e);
//                    }
//                    //创建Elasticsearch指标表
//                    clientInterface.createIndiceMapping("vops-loginmodulemetrics","vops-loginmodulemetrics-dsl");
//                    clientInterface.createIndiceMapping("vops-loginusermetrics","vops-loginusermetrics-dsl");
//                }
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
                        if(resourceStartResult instanceof DBStartResult) { //作业停止时，释放db数据源
                            DataTranPluginImpl.stopDatasources((DBStartResult) resourceStartResult);
                        }

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

        ElasticsearchInputConfig elasticsearchInputConfig = new ElasticsearchInputConfig();
        elasticsearchInputConfig.setDslFile("dsl2ndSqlFile.xml")//配置dsl和sql语句的配置文件
                .setDslName("scrollQuery") //指定从es查询索引文档数据的dsl语句名称，配置在dsl2ndSqlFile.xml中
                .setScrollLiveTime("10m") //scroll查询的scrollid有效期

//					 .setSliceQuery(true)
//				     .setSliceSize(5)
                .setQueryUrl("dbdemo/_search")
                .addSourceElasticsearch("elasticsearch.serverNames","default")
                .addElasticsearchProperty("default.elasticsearch.rest.hostNames","192.168.137.1:9200")
                .addElasticsearchProperty("default.elasticsearch.showTemplate","true")
                .addElasticsearchProperty("default.elasticUser","elastic")
                .addElasticsearchProperty("default.elasticPassword","changeme")
                .addElasticsearchProperty("default.elasticsearch.failAllContinue","true")
                .addElasticsearchProperty("default.http.timeoutSocket","60000")
                .addElasticsearchProperty("default.http.timeoutConnection","40000")
                .addElasticsearchProperty("default.http.connectionRequestTimeout","70000")
                .addElasticsearchProperty("default.http.maxTotal","200")
                .addElasticsearchProperty("default.http.defaultMaxPerRoute","100");//查询索引表demo中的文档数据
                
//				//添加dsl中需要用到的参数及参数值
//				exportBuilder.addParam("var1","v1")
//				.addParam("var2","v2")
//				.addParam("var3","v3");
        importBuilder.addJobInputParam("v1","sss");
        importBuilder.addJobDynamicInputParam("aaaa", new DynamicParam() {
            @Override
            public Object getValue(String paramName, DynamicParamContext dynamicParamContext) {
                return new Date();
            }
        });

        importBuilder.setIncreamentEndOffset(50);
        importBuilder.setInputConfig(elasticsearchInputConfig);




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
                metric(metricKey, mapData, LoginModuleMetric::new);

                //指标2 按照用户统计操作次数
                String logUser = (String) data.getData("logOperuser");
                metricKey = logUser;
                metric(metricKey, mapData, LoginUserMetric::new);


            }

            @Override
            public void persistent(Collection< KeyMetric> metrics) {
                metrics.forEach(keyMetric->{
                    if(keyMetric instanceof LoginModuleMetric) {
                        LoginModuleMetric testKeyMetric = (LoginModuleMetric) keyMetric;
//                        Map esData = new HashMap();
//                        esData.put("dataTime", testKeyMetric.getDataTime());
//                        esData.put("hour", testKeyMetric.getDayHour());
//                        esData.put("minute", testKeyMetric.getMinute());
//                        esData.put("day", testKeyMetric.getDay());
//                        esData.put("metric", testKeyMetric.getMetric());
//                        esData.put("operModule", testKeyMetric.getOperModule());
//                        esData.put("count", testKeyMetric.getCount());
//                        objectHolder.getObject().insertData("vops-loginmodulemetrics", esData);
                        objectHolder.getObject().insertData( testKeyMetric);
                    }
                    else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
//                        Map esData = new HashMap();
//                        esData.put("dataTime", testKeyMetric.getDataTime());
//                        esData.put("hour", testKeyMetric.getDayHour());
//                        esData.put("minute", testKeyMetric.getMinute());
//                        esData.put("day", testKeyMetric.getDay());
//                        esData.put("metric", testKeyMetric.getMetric());
//                        esData.put("logUser", testKeyMetric.getLogUser());
//                        esData.put("count", testKeyMetric.getCount());
//                        objectHolder.getObject().insertData("vops-loginusermetrics", esData);
                        objectHolder.getObject().insertData( testKeyMetric);
                    }

                });

            }
        };
        keyMetrics.setTimeWindows(3600);
        MetricsOutputConfig metricsOutputConfig = new MetricsOutputConfig();

        metricsOutputConfig.setDataTimeField("time");
        
        metricsOutputConfig.addMetrics(keyMetrics);

        importBuilder.setFlushMetricsOnScheduleTaskCompleted(true);
        importBuilder.setWaitCompleteWhenflushMetricsOnScheduleTaskCompleted(true);
        importBuilder.setCleanKeysWhenflushMetricsOnScheduleTaskCompleted(true);

        importBuilder.setOutputConfig(metricsOutputConfig);

		/**
		 * 设置IP地址信息库
		 */
		importBuilder.setGeoipDatabase("C:/workdir/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("C:/workdir/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("C:/workdir/geolite2/ip2region.db");
//定时任务配置，
        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
                .setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
                .setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次

        //增量配置开始
		importBuilder.setLastValueColumn("time");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
//		importBuilder.setDateLastValueColumn("log_id");//手动指定日期增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
        importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
        //setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
        importBuilder.setLastValueStorePath("logdb2db_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
        importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
        // 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
        //增量配置结束
        //定时任务配置结束
		importBuilder
//
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

	 
		//定时任务配置结束

//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
//		importBuilder.setStatusDbname("test");//设置增量状态数据源名称
		 
//		setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
//		importBuilder.setStatusDbname("testStatus");//指定增量状态数据源名称
		 
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
				//添加用于指标计算处理等的临时数据到记录，不会对临时数据进行持久化处理，
				context.addTempData("name","ddd");

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

		importBuilder.setExportResultHandler(new ExportResultHandler() {
			@Override
			public void success(TaskCommand taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
//				logger.info(taskMetrics.toString());
				logger.debug(result+"");
			}

			@Override
			public void error(TaskCommand taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
				logger.warn(result+"");
			}

			@Override
			public void exception(TaskCommand taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.warn(taskMetrics.toString());
			}


		});


		/**
		 * 构建和执行数据库表数据导入es和指标统计作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();


	}


}
