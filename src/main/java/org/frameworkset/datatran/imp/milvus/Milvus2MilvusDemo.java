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

import com.frameworkset.util.SimpleStringUtil;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import org.frameworkset.nosql.milvus.MilvusConfig;
import org.frameworkset.nosql.milvus.MilvusFunction;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.nosql.milvus.MilvusStartResult;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.spi.remote.http.HttpResourceStartResult;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.milvus.input.MilvusInputConfig;
import org.frameworkset.tran.plugin.milvus.output.MilvusOutputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>Description: 基于数字类型字段log_id增量同步采集Milvus向量数据库源表demo的数据到Milvus向量库目标表targetdemo
 * </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Milvus2MilvusDemo {
	private static final Logger logger = LoggerFactory.getLogger(Milvus2MilvusDemo.class);
	public static void main(String[] args){
		Milvus2MilvusDemo dbdemo = new Milvus2MilvusDemo();
		dbdemo.scheduleImportData();
	}

 
	public void scheduleImportData(){
		ImportBuilder importBuilder = ImportBuilder.newInstance();
        importBuilder.setJobId("Milvus2MilvusDemo");
        /**
         * 设置增量状态ID生成策略，在设置jobId的情况下起作用
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用[jobType]+[jobId]+[作业查询语句/文件路径等信息的hashcode]，作为增量id作为增量状态id
         * 默认值ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT
         */
        importBuilder.setStatusIdPolicy(ImportIncreamentConfig.STATUSID_POLICY_JOBID);

        String targetCollectionName = "targetdemo";
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {
                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        //初始化milvus数据源服务，用来操作向量数据库
                        MilvusConfig milvusConfig = new MilvusConfig();
                        milvusConfig.setName("ucr_chan_fqa");//数据源名称
                        milvusConfig.setDbName("ucr_chan_fqa");//Milvus数据库名称
                        milvusConfig.setUri("http://172.24.176.18:19530");//Milvus数据库地址
                        milvusConfig.setToken("");//认证token：root:xxxx
                        ResourceStartResult resourceStartResult =  MilvusHelper.init(milvusConfig);//加载配置初始化Milvus数据源
                        //如果向量表不存在，则创建向量表targetCollectionName
                        MilvusHelper.executeRequest("ucr_chan_fqa", new MilvusFunction<Void>() {
                            @Override
                            public Void execute(MilvusClientV2 milvusClientV2) {
                                if(!milvusClientV2.hasCollection(HasCollectionReq.builder()
                                        .collectionName(targetCollectionName)
                                        .build())) {
                                    ;
                                    // create a collection with schema, when indexParams is specified, it will create index as well
                                    CreateCollectionReq.CollectionSchema collectionSchema = milvusClientV2.createSchema();
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("log_id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE)
                                            .autoID(Boolean.FALSE).build());//主键
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("content").dataType(DataType.FloatVector).dimension(1024).build());//日志内容对应的向量值
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("collecttime").dataType(DataType.Int64).build());//日志采集时间
                                    collectionSchema.addField(AddFieldReq.builder().fieldName("log_content").dataType(DataType.VarChar).build());//日志内容原始值
                                    IndexParam indexParam = IndexParam.builder()
                                            .fieldName("content")
                                            .metricType(IndexParam.MetricType.COSINE)
                                            .build();
                                    CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                                            .collectionName(targetCollectionName)
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
                        //销毁初始化阶段自定义的向量数据库数据源
                        if(resourceStartResult instanceof MilvusStartResult){
                            MilvusHelper.shutdown((MilvusStartResult) resourceStartResult);
                        }
                    }
                });
            }
        });


		/**
		 * 源Milvus相关配置，这里用与目标库相同的Milvus数据源ucr_chan_fqa（在startaction中初始化）
		 */
        String[] array = {"log_id","collecttime","log_content","content"};//定义要返回的字段清单
		MilvusInputConfig milvusInputConfig = new MilvusInputConfig();
		milvusInputConfig.setName("ucr_chan_fqa")  //使用之前定义的向量数据库数据源，无需设置向量数据库地址和名称以及token等信息
//                             .setDbName("ucr_chan_fqa")
                            .setExpr("log_id < 100000")//指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
//                             .setUri("http://172.24.176.18:19530").setToken("")
                            .setOutputFields(Arrays.asList(array))  //设置返回字段清单                           
                             .setCollectionName("demo");//指定源表名称
		importBuilder.setInputConfig(milvusInputConfig);

        /**
         * 目标Milvus配置，这里用与源相同的Milvus数据源ucr_chan_fqa（在startaction中初始化）
         */
        MilvusOutputConfig milvusOutputConfig = new MilvusOutputConfig();
        milvusOutputConfig.setName("ucr_chan_fqa")  //使用之前定义的向量数据库数据源，无需设置向量数据库地址和名称以及token等信息
//                             .setDbName("ucr_chan_fqa")
//                             .setUri("http://172.24.176.18:19530")
//                             .setToken("")
                .setCollectionName(targetCollectionName)
                .setLoadCollectionSchema(true)
                .setUpsert(true);//存在更新，不存在则插入
        importBuilder.setOutputConfig(milvusOutputConfig);

        importBuilder.setFetchSize(1000); // 批量从Milvus拉取记录数
        importBuilder.setBatchSize(50); //可选项,批量输出Milvus记录数
		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束
  
//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
		importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
//		importBuilder.setDateLastValueColumn("log_id");//手动指定日期增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("Milvus2MilvusDemo_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
        importBuilder.setLastValue(-100000);
		//增量配置结束
 
		
		
		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

		importBuilder.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
		importBuilder.setExportResultHandler(new ExportResultHandler<Object> () {
			@Override
			public void success(TaskCommand<Object> taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                if(result != null) {
                    logger.info(result.toString());
                }
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
		 * 执行Milvus数据迁移作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作

		logger.info("come to end.");


	}

}
