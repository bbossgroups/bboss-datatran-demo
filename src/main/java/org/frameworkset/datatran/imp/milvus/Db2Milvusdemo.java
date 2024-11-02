package org.frameworkset.datatran.imp.milvus;
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

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import org.frameworkset.nosql.milvus.*;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.spi.remote.http.HttpResourceStartResult;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.milvus.output.MilvusOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Description: 同步处理程序，如需调试同步功能，
 * 请运行测试用例DbdemoTest中调试</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2Milvusdemo {
	private static final Logger logger = LoggerFactory.getLogger(Db2Milvusdemo.class);
	public static void main(String[] args){
		Db2Milvusdemo dbdemo = new Db2Milvusdemo();
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
        importBuilder.setJobId("Db2Milvusdemo");
        /**
         * 设置增量状态ID生成策略，在设置jobId的情况下起作用
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用[jobType]+[jobId]+[作业查询语句/文件路径等信息的hashcode]，作为增量id作为增量状态id
         * 默认值ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT
         */
        importBuilder.setStatusIdPolicy(ImportIncreamentConfig.STATUSID_POLICY_JOBID);
        String collectionName = "demo";
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {
                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        //初始化向量模型服务
                        Map properties = new HashMap();

                        //embedding_model为的向量模型服务数据源名称
                        properties.put("http.poolNames","embedding_model");
                    
                        properties.put("embedding_model.http.hosts","127.0.0.1:7861");//设置向量模型服务地址，这里调用的xinference发布的模型服务
              
                        properties.put("embedding_model.http.timeoutSocket","60000");
                        properties.put("embedding_model.http.timeoutConnection","40000");
                        properties.put("embedding_model.http.connectionRequestTimeout","70000");
                        properties.put("embedding_model.http.maxTotal","100");
                        properties.put("embedding_model.http.defaultMaxPerRoute","100");
                        return HttpRequestProxy.startHttpPools(properties);

                    }
                }).addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        //初始化milvus数据源服务，用来操作向量数据库
                        MilvusConfig milvusConfig = new MilvusConfig();
                        milvusConfig.setName("ucr_chan_fqa");
                        milvusConfig.setDbName("ucr_chan_fqa");
                        milvusConfig.setUri("http://172.24.176.18:19530");
                        milvusConfig.setToken("");
                        ResourceStartResult resourceStartResult =  MilvusHelper.init(milvusConfig);
                        //如果向量表不存在，则创建向量表
                        MilvusHelper.executeRequest("ucr_chan_fqa", new MilvusFunction<Void>() {
                            @Override
                            public Void execute(MilvusClientV2 milvusClientV2) {
                                if(!milvusClientV2.hasCollection(HasCollectionReq.builder()
                                        .collectionName(collectionName)
                                        .build())) {
                                    ;
                                    // create a collection with schema, when indexParams is specified, it will create index as well
                                    CreateCollectionReq.CollectionSchema collectionSchema = milvusClientV2.createSchema();
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("log_id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).autoID(Boolean.FALSE).build());
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("content").dataType(DataType.FloatVector).dimension(1024).build());
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("collecttime").dataType(DataType.Int64).build());

                                    IndexParam indexParam = IndexParam.builder()
                                            .fieldName("content")
                                            .metricType(IndexParam.MetricType.COSINE)
                                            .build();
                                    CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                                            .collectionName(collectionName)
                                            .collectionSchema(collectionSchema)
                                            .indexParams(Collections.singletonList(indexParam))
                                            .build();
                                    milvusClientV2.createCollection(createCollectionReq);
                                }
                                return null;
                            }
                        });
                        return resourceStartResult;
                    }
                });
            }

            @Override
            public void afterStartAction(ImportContext importContext) {
            }
        });
        //作业结束后销毁初始化阶段自定义的向量模型服务数据源和向量数据库数据源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {
                
                //销毁初始化阶段自定义的数据源
                importContext.destroyResources(new ResourceEnd() {
                    @Override
                    public void endResource(ResourceStartResult resourceStartResult) {

                        //销毁初始化阶段自定义的向量模型服务数据源
                        if (resourceStartResult instanceof HttpResourceStartResult) {
                            HttpResourceStartResult httpResourceStartResult = (HttpResourceStartResult) resourceStartResult;
                            HttpRequestProxy.stopHttpClients(httpResourceStartResult);
                        }
                        //销毁初始化阶段自定义的向量数据库数据源
                        else if(resourceStartResult instanceof MilvusStartResult){
                            MilvusHelper.shutdown((MilvusStartResult) resourceStartResult);
                        }
                    }
                });
            }
        });
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

		MilvusOutputConfig milvusOutputConfig = new MilvusOutputConfig();
        milvusOutputConfig.setName("ucr_chan_fqa")  //使用之前定义的向量数据库数据源，无需设置向量数据库地址和名称以及token等信息
//                             .setDbName("ucr_chan_fqa")
//                             .setUri("http://172.24.176.18:19530")
//                             .setToken("")
                          .setCollectionName(collectionName)
                          .setLoadCollectionSchema(true)
                            .setUpsert(true);
		importBuilder.setOutputConfig(milvusOutputConfig);

	 
		importBuilder.setBatchSize(50); //可选项,批量导入db的记录数，默认为-1，逐条处理，> 0时批量处理
		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束
  
//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
		importBuilder.setLastValueColumn("LOG_ID");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
//		importBuilder.setDateLastValueColumn("log_id");//手动指定日期增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("logdb2milvus_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//增量配置结束

 
//
		final AtomicInteger s = new AtomicInteger(0);
		importBuilder.setGeoipDatabase("c:/data/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("c:/data/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("c:/data/geolite2/ip2region.db");
		/**
		 * 加工和处理数据
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
 
               String content = context.getStringValue("LOG_CONTENT");
               if(content != null){
                   Map params = new HashMap();
                   params.put("text",content);
                   //调用向量服务，将LOG_CONTENT转换为向量数据
                   BaseResponse baseResponse = HttpRequestProxy.sendJsonBody("embedding_model",params,"/fqa-py-api/knowledge_base/kb_embedding_textv1",BaseResponse.class);
                   if(baseResponse.getCode() == 200){
                       context.addFieldValue("content",baseResponse.getData());//设置向量数据
                   }
                   else {
                       throw new DataImportException("change LOG_CONTENT to vector failed:"+baseResponse.getMsg());
                   }
               }
               //添加主键信息
               int log_id = context.getIntegerValue("LOG_ID");
                context.addFieldValue("log_id",log_id);
                //添加采集时间
				context.addFieldValue("collecttime",new Date().getTime());

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
		importBuilder.setExportResultHandler(new ExportResultHandler<Object> () {
			@Override
			public void success(TaskCommand<Object> taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(result.toString());
			}

			@Override
			public void error(TaskCommand<Object> taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
			}

			@Override
			public void exception(TaskCommand<Object> taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.error(taskMetrics.toString(),exception);
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
