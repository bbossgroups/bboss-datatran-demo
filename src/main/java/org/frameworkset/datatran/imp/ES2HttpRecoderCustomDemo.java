package org.frameworkset.datatran.imp;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.InitJobContextCall;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.es.input.ElasticsearchInputConfig;
import org.frameworkset.tran.plugin.http.output.HttpOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/1
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2HttpRecoderCustomDemo {
	private static Logger logger = LoggerFactory.getLogger(ES2HttpRecoderCustomDemo.class);
	public static void main(String[] args){


		ImportBuilder importBuilder = new ImportBuilder() ;
		importBuilder.setFetchSize(50).setBatchSize(10);
		ElasticsearchInputConfig elasticsearchInputConfig = new ElasticsearchInputConfig();
		elasticsearchInputConfig.setDslFile("dsl2ndSqlFile.xml")//配置dsl和sql语句的配置文件
				.setDslName("scrollQuery") //指定从es查询索引文档数据的dsl语句名称，配置在dsl2ndSqlFile.xml中
				.setScrollLiveTime("10m") //scroll查询的scrollid有效期

//					 .setSliceQuery(true)
//				     .setSliceSize(5)
				.setQueryUrl("https2es/_search")
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

		importBuilder.setInputConfig(elasticsearchInputConfig);
		HttpOutputConfig httpOutputConfig = new HttpOutputConfig();
		//指定导入数据的dsl语句，必填项，可以设置自己的提取逻辑，
		// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：


		httpOutputConfig.setJson(false)
				.setLineSeparator("|")
				.setRecordGenerator(new RecordGenerator() {
					@Override
					public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) throws Exception {
						Map<String, Object> datas = record.getDatas();
						try {
							Map<String,String> chanMap = (Map<String,String>)taskContext.getTaskData("chanMap");

							String phoneNumber = (String) datas.get("phoneNumber");//手机号码
							if(phoneNumber==null){
								phoneNumber="";
							}
							builder.write(phoneNumber);
							builder.write("|");

							String chanId = (String) datas.get("chanId");//办理渠道名称 通过Id获取名称
							String chanName = null;
							if(chanId==null){
								chanName="";
							}else{
								chanName=chanMap.get(chanId);
								if(chanName == null){
									chanName = chanId;
								}
							}
							builder.write(chanName);
							builder.write("|");

							String startTime = "";//办理开始时间(时间戳)
							if( datas.get("startTime")!=null){
								startTime=datas.get("startTime")+"";
							}
							builder.write(startTime);
							builder.write("|");

							String endTime = "";//办理结束时间(时间戳)
							if( datas.get("endTime")!=null){
								endTime=datas.get("endTime")+"";
							}
							builder.write(endTime);
							builder.write("|");

							String ydCodeLv1 = (String) datas.get("ydCodeLv1");//业务一级分类编码（取目前的业务大类编码）
							if(ydCodeLv1==null){
								ydCodeLv1="";
							}
							builder.write(ydCodeLv1);
							builder.write("|");

							String ydNameLv1 = (String) datas.get("ydNameLv1");//业务一级分类名称（取目前的业务大类名称）
							if(ydNameLv1==null){
								ydNameLv1="";
							}
							builder.write(ydNameLv1);
							builder.write("|");

							String ydCodeLv2 = (String) datas.get("ydCodeLv2");//业务二级分类编码（取目前的业务小类编码）
							if(ydCodeLv2==null){
								ydCodeLv2="";
							}
							builder.write(ydCodeLv2);
							builder.write("|");

							String ydNameLv2 = (String) datas.get("ydNameLv2");//、业务二级分类名称（取目前的业务小类名称）
							if(ydNameLv2==null){
								ydNameLv2="";
							}
							builder.write(ydNameLv2);
							builder.write("|");

							String ydCodeLv3 = (String) datas.get("ydCodeLv3");//业务三级分类编码（取目前的产品编码）
							if(ydCodeLv3==null){
								ydCodeLv3="";
							}
							builder.write(ydCodeLv3);
							builder.write("|");

							String ydNameLv3 = (String) datas.get("ydNameLv3");//业务三级分类名称（取目前的产品名称）
							if(ydNameLv3==null){
								ydNameLv3="";
							}
							builder.write(ydNameLv3);
							builder.write("|");

							String goodsName = (String) datas.get("goodsName");//资费档次名称
							if(goodsName==null){
								goodsName="";
							}
							builder.write(goodsName);
							builder.write("|");

							String goodsCode = (String) datas.get("goodsCode");//资费档次编码
							if(goodsCode==null){
								goodsCode="";
							}
							builder.write(goodsCode);
							builder.write("|");

							String bossErrorCode = (String) datas.get("bossErrorCode");//BOSS错误码
							if(bossErrorCode==null){
								bossErrorCode="";
							}
							builder.write(bossErrorCode);
							builder.write("|");

							String bossErrorDesc = (String) datas.get("bossErrorDesc");//BOSS错误码描述
							if(bossErrorDesc==null){
								bossErrorDesc="";
							}else{
								bossErrorDesc = bossErrorDesc.replace("|","__").replace("\r\n","");
							}
							builder.write(bossErrorDesc);

						} catch (IOException e) {
							throw new DataImportException("RecordGenerator failed:",e);
						}
					}
				})
				.setServiceUrl("/httpservice/sendData.api")
				.setHttpMethod("post")
				.addTargetHttpPoolName("http.poolNames","datatran")
				.addHttpOutputConfig("datatran.http.health","/health")
				.addHttpOutputConfig("datatran.http.hosts","192.168.137.1:808")
				.addHttpOutputConfig("datatran.http.timeoutConnection","5000")
				.addHttpOutputConfig("datatran.http.timeoutSocket","50000")
				.addHttpOutputConfig("datatran.http.connectionRequestTimeout","50000")
				.addHttpOutputConfig("datatran.http.maxTotal","200")
				.addHttpOutputConfig("datatran.http.defaultMaxPerRoute","100")
				.addHttpOutputConfig("datatran.http.failAllContinue","true");

		importBuilder.setOutputConfig(httpOutputConfig);


		importBuilder
//
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
				.setUseLowcase(true)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				;  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(1000L * 60); //每隔period毫秒执行，如果不设置，只执行一次
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
		//增量配置开始
//		importBuilder.setStatusDbname("test");//设置增量状态数据源名称
		importBuilder.setLastValueColumn("collecttime");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
//		setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setStatusDbname("es2http");
		importBuilder.setLastValueStorePath("es2http_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
		importBuilder.setLastValueStoreTableName("es2http");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
		importBuilder.setIncreamentEndOffset(60*1);//单位：秒
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//		try {
//			Date date = format.parse("2000-01-01");
//			importBuilder.setLastValue(date);//增量起始值配置
//		}
//		catch (Exception e){
//			e.printStackTrace();
//		}
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//增量配置结束

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
//				long logTime = context.getLongValue("logTime");
//				context.addFieldValue("logTime",new Date(logTime));
//				long oldLogTime = context.getLongValue("oldLogTime");
//				context.addFieldValue("oldLogTime",new Date(oldLogTime));
//				long oldLogTimeEndTime = context.getLongValue("oldLogTimeEndTime");
//				context.addFieldValue("oldLogTimeEndTime",new Date(oldLogTimeEndTime));
//				Date date = context.getDateValue("LOG_OPERTIME");

				Object value = context.getJobContext().getJobData("test");
				context.addFieldValue("newCollecttime",new Date());//添加采集时间

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
				Object value = taskCommand.getJobContext().getJobData("test");
				logger.info(taskMetrics.toString());
				logger.debug(result);
			}

			@Override
			public void error(TaskCommand<String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				Object value = taskCommand.getJobContext().getJobData("test");
				logger.info(taskMetrics.toString());
				logger.debug(result);
			}

			@Override
			public void exception(TaskCommand<String> taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				Object value = taskCommand.getJobContext().getJobData("test");
				logger.debug(taskMetrics.toString());
			}


		});
		/**
		 * 执行数据库表数据导入es操作
		 */
		importBuilder.setInitJobContextCall(new InitJobContextCall() {
			@Override
			public void initJobContext(JobContext jobContext) {
				jobContext.addJobData("test",1111);
			}
		});
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作
	}
}
