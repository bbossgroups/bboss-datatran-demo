package org.frameworkset.datatran.imp.metrics;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FileTaskContext;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.beans.ObjectHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: 从Ftp下载并采集日志文件，将采集的日志数据经过加工处理后保存到elasticsearch，同时进行流计算处理，将计算的指标结果异步bulk写入Elasticsearch</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class SFTPFileLog2ESWithMetricsDemo {
	private static Logger logger = LoggerFactory.getLogger(SFTPFileLog2ESWithMetricsDemo.class);
	public static void main(String[] args){

//		Pattern pattern = Pattern.compile("(?!.*(endpoint)).*");
//		logger.info(""+pattern.matcher("xxxxsssssssss").find());
//		logger.info(""+pattern.matcher("xxxxsssendpointssssss").find());
		try {
//			ElasticSearchHelper.getRestClientUtil().getDocumentByField("xxxx-*","requestId","xxxx");
			//清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
//			String repsonse = ElasticSearchHelper.getRestClientUtil().dropIndice("errorlog");
			String repsonse = ElasticSearchHelper.getRestClientUtil().dropIndice("metrics-report");
			logger.info(repsonse);
		} catch (Exception e) {
		}
		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setBatchSize(40)//设置批量入库的记录数
				.setFetchSize(1000);//设置按批读取文件行数
		//设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
		importBuilder.setFlushInterval(10000l);
//		importBuilder.setSplitFieldName("@message");
//		importBuilder.setSplitHandler(new SplitHandler() {
//			@Override
//			public List<KeyMap<String, Object>> splitField(TaskContext taskContext,
//														   Record record, Object splitValue) {
//				Map<String,Object > data = (Map<String, Object>) record.getData();
//				List<KeyMap<String, Object>> splitDatas = new ArrayList<>();
//				//模拟将数据切割为10条记录
//				for(int i = 0 ; i < 10; i ++){
//					KeyMap<String, Object> d = new KeyMap<String, Object>();
//					d.put("message",i+"-"+(String)data.get("@message"));
////					d.setKey(SimpleStringUtil.getUUID());//如果是往kafka推送数据，可以设置推送的key
//					splitDatas.add(d);
//				}
//				return splitDatas;
//			}
//		});
		FileInputConfig fileInputConfig = new FileInputConfig();
		fileInputConfig.setCharsetEncode("GB2312");
        fileInputConfig.setMaxFilesThreshold(50);
		//.*.txt.[0-9]+$
		//[17:21:32:388]
//		config.addConfig(new FileConfig("D:\\ecslog",//指定目录
//				"error-2021-03-27-1.log",//指定文件名称，可以是正则表达式
//				"^\\[[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
//				.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
//				.setMaxBytes(1048576)//控制每条日志的最大长度，超过长度将被截取掉
//				//.setStartPointer(1000l)//设置采集的起始位置，日志内容偏移量
//				.addField("tag","error") //添加字段tag到记录中
//				.setExcludeLines(new String[]{"\\[DEBUG\\]"}));//不采集debug日志

//		config.addConfig(new FileConfig("D:\\workspace\\bbossesdemo\\filelog-elasticsearch\\",//指定目录
//				"es.log",//指定文件名称，可以是正则表达式
//				"^\\[[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
//				.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
//				.addField("tag","elasticsearch")//添加字段tag到记录中
//				.setEnableInode(false)
////				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
//				//.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
//		);
//		config.addConfig(new FileConfig("D:\\workspace\\bbossesdemo\\filelog-elasticsearch\\",//指定目录
//						new FileFilter() {
//							@Override
//							public boolean accept(File dir, String name, FileConfig fileConfig) {
//								//判断是否采集文件数据，返回true标识采集，false 不采集
//								return name.equals("es.log");
//							}
//						},//指定文件过滤器
//						"^\\[[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
//						.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
//						.addField("tag","elasticsearch")//添加字段tag到记录中
//						.setEnableInode(false)
////				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
//				//.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
//		);
        FileConfig fileConfig = new FileConfig();
        fileConfig.setFieldSplit(";");//指定日志记录字段分割符
        //指定字段映射配置
        fileConfig.addCellMapping(0, "logOperTime");

        fileConfig.addCellMapping(1, "operModule");
        fileConfig.addCellMapping(2, "logOperuser");
//        fileConfig.addDateCellMapping(0, "logOperTime", CELL_STRING, "2022-08-09 12:30:50", "yyyy-MM-dd HH:mm:ss");//指定数据类型，默认值，日期格式
//        fileConfig.addCellMappingWithType(1, "operModule", CELL_STRING );//指定数据类型
//        fileConfig.addCellMappingWithType(2, "logOperuser", CELL_NUMBER_INTEGER, 20);//指定数据类型和默认值

        FtpConfig ftpConfig = new FtpConfig().setFtpIP("101.13.6.55").setFtpPort(233)
                .setFtpUser("1q").setFtpPassword("123")
                .setRemoteFileDir("/home/ecs/failLog")
                .setTransferProtocol(FtpConfig.TRANSFER_PROTOCOL_SFTP) ;//采用sftp协议
		fileInputConfig.addConfig(fileConfig.setFtpConfig(ftpConfig).setSourcePath("D:\\logs")//指定目录
										.setFileHeadLineRegular("^\\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
										.setFileFilter(new FileFilter() {
											@Override
											public boolean accept(FilterFileInfo fileInfo, FileConfig fileConfig) {
												//判断是否采集文件数据，返回true标识采集，false 不采集
												return fileInfo.getFileName().equals("metrics-report.log");
											}
										})//指定文件过滤器
										.addField("tag","elasticsearch")//添加字段tag到记录中
                                        .setScanChild(true)
										.setEnableInode(false)
				//				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
								//.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
						);

//		config.addConfig("E:\\ELK\\data\\data3",".*.txt","^[0-9]{4}-[0-9]{2}-[0-9]{2}");
		/**
		 * 启用元数据信息到记录中，元数据信息以map结构方式作为@filemeta字段值添加到记录中，文件插件支持的元信息字段如下：
		 * hostIp：主机ip
		 * hostName：主机名称
		 * filePath： 文件路径
		 * timestamp：采集的时间戳
		 * pointer：记录对应的截止文件指针,long类型
		 * fileId：linux文件号，windows系统对应文件路径
		 * 例如：
		 * {
		 *   "_index": "filelog",
		 *   "_type": "_doc",
		 *   "_id": "HKErgXgBivowv_nD0Jhn",
		 *   "_version": 1,
		 *   "_score": null,
		 *   "_source": {
		 *     "title": "解放",
		 *     "subtitle": "小康",
		 *     "ipinfo": "",
		 *     "newcollecttime": "2021-03-30T03:27:04.546Z",
		 *     "author": "张无忌",
		 *     "@filemeta": {
		 *       "path": "D:\\ecslog\\error-2021-03-27-1.log",
		 *       "hostname": "",
		 *       "pointer": 3342583,
		 *       "hostip": "",
		 *       "timestamp": 1617074824542,
		 *       "fileId": "D:/ecslog/error-2021-03-27-1.log"
		 *     },
		 *     "message": "[18:04:40:161] [INFO] - org.frameworkset.tran.schedule.ScheduleService.externalTimeSchedule(ScheduleService.java:192) - Execute schedule job Take 3 ms"
		 *   }
		 * }
		 *
		 * true 开启 false 关闭
		 */
		fileInputConfig.setEnableMeta(true);

		importBuilder.setInputConfig(fileInputConfig);
		//指定elasticsearch数据源名称，在application.properties文件中配置，default为默认的es数据源名称
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
		elasticsearchOutputConfig.setTargetElasticsearch("default");
		//指定索引名称，这里采用的是elasticsearch 7以上的版本进行测试，不需要指定type
		elasticsearchOutputConfig.setIndex("metrics-report");
		//指定索引类型，这里采用的是elasticsearch 7以上的版本进行测试，不需要指定type
		//elasticsearchOutputConfig.setIndexType("idxtype");
		importBuilder.setOutputConfig(elasticsearchOutputConfig);


        //流处理配置开始

        /**
         * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
         */
        ObjectHolder<BulkProcessor> objectHolder = new ObjectHolder<BulkProcessor>();
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {

            }

            @Override
            public void afterStartAction(ImportContext importContext) {
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
                        esData.put("dataTime", testKeyMetric.getDataTime());//logOpertime字段值
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("operModule", testKeyMetric.getOperModule());
                        esData.put("count", testKeyMetric.getCount());
                        objectHolder.getObject().insertData("vops-loginmodulemetrics", esData);//将指标计算结果异步批量写入Elasticsearch表vops-loginmodulemetrics
                    }
                    else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());//logOpertime字段值
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("logUser", testKeyMetric.getLogUser());
                        esData.put("count", testKeyMetric.getCount());
                        objectHolder.getObject().insertData("vops-loginusermetrics", esData);//将指标计算结果异步批量写入Elasticsearch表vops-loginusermetrics
                    }

                });

            }
        };


        importBuilder.setDataTimeField("logOpertime");//设置指标统计时间维度字段
        importBuilder.addMetrics(keyMetrics);
        //流处理配置结束

		//增量配置开始
		importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("fileloges_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
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
		importBuilder.addFieldValue("author","张无忌");
//		importBuilder.addFieldMapping("operModule","OPER_MODULE");
//		importBuilder.addFieldMapping("logContent","LOG_CONTENT");


		/**
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
//				System.out.println(data);

//				context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");
				

				//直接获取文件元信息
				Map fileMata = (Map)context.getValue("@filemeta");
				/**
				 * 文件插件支持的元信息字段如下：
				 * hostIp：主机ip
				 * hostName：主机名称
				 * filePath： 文件路径
				 * timestamp：采集的时间戳
				 * pointer：记录对应的截止文件指针,long类型
				 * fileId：linux文件号，windows系统对应文件路径
				 */
				String filePath = (String)context.getMetaValue("filePath");
				//可以根据文件路径信息设置不同的索引
//				if(filePath.endsWith("metrics-report.log")) {
//					context.setIndex("metrics-report");
//				}
//				else if(filePath.endsWith("es.log")){
//					 context.setIndex("eslog");
//				}


//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				/**
				IpInfo ipInfo = (IpInfo) context.getIpInfo(fvs[2]);
				if(ipInfo != null)
					context.addFieldValue("ipinfo", ipInfo);
				else{
					context.addFieldValue("ipinfo", "");
				}*/
				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
//				Date optime = context.getDateValue("LOG_OPERTIME",dateFormat);
				context.addFieldValue("logOpertime",new Date());
				context.addFieldValue("collecttime",new Date());

				/**
				 //关联查询数据,单值查询
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
			}
		});
		//映射和转换配置结束
		importBuilder.setExportResultHandler(new ExportResultHandler() {
			@Override
			public void success(TaskCommand taskCommand, Object o) {
				logger.info("result:"+o);
			}

			@Override
			public void error(TaskCommand taskCommand, Object o) {
				logger.warn("error:"+o);
			}

			@Override
			public void exception(TaskCommand taskCommand, Throwable exception) {
				logger.warn("error:",exception);
			}


		});
		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(true);

		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {

			}

			@Override
			public void afterCall(TaskContext taskContext) {
				if(taskContext != null) {
					FileTaskContext fileTaskContext = (FileTaskContext)taskContext;
					logger.info("文件{}导入情况:{}",fileTaskContext.getFileInfo().getOriginFilePath(),taskContext.getJobTaskMetrics().toString());
				}
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				if(taskContext != null) {
					FileTaskContext fileTaskContext = (FileTaskContext)taskContext;
					logger.info("文件{}导入情况:{}",fileTaskContext.getFileInfo().getOriginFilePath(),taskContext.getJobTaskMetrics().toString());
				}
			}
		});
		/**
		 * 构建作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//启动同步作业
		logger.info("job started.");
	}
}
