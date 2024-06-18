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

import com.frameworkset.util.BaseSimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.boot.ElasticsearchBootResult;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.util.beans.ObjectHolder;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FileTaskContext;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.plugin.metrics.output.BuildMapData;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsData;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: 从日志文件采集日志数据并保存到elasticsearch，同时进行流计算处理，将计算的指标结果异步bulk写入Elasticsearch</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class FileLog2ESWithMetricsCustomDateTimeDemo {
	private static Logger logger = LoggerFactory.getLogger(FileLog2ESWithMetricsCustomDateTimeDemo.class);
	public static void main(String[] args){


		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setBatchSize(100)//设置批量入库的记录数
				.setFetchSize(1000);//设置按批读取文件行数
		//设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
		importBuilder.setFlushInterval(10000l);



        //流处理配置开始

        /**
         * 在作业初始化时，构建持久化指标数据的BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
         * 将构建好的BulkProcessor实例放入ObjectHolder，以便后续持久化指标数据时使用
         *
         */
        ObjectHolder<BulkProcessor> bulkProcessorHolder = new ObjectHolder<BulkProcessor>();
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {
                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        //bulkprocessor Elasticsearch数据源，进行数据源初始化定义
                        Map properties = new HashMap();

                        //metricsElasticsearch为Elasitcsearch数据源名称,在指标数据的BulkProcessor批处理组件使用
                        properties.put("elasticsearch.serverNames","metricsElasticsearch");

                        /**
                         * metricsElasticsearch数据源配置，每个配置项可以加metricsElasticsearch.前缀
                         */


                        properties.put("metricsElasticsearch.elasticsearch.rest.hostNames","192.168.137.1:9200");
                        properties.put("metricsElasticsearch.elasticsearch.showTemplate","true");
                        properties.put("metricsElasticsearch.elasticUser","elastic");
                        properties.put("metricsElasticsearch.elasticPassword","changeme");
                        properties.put("metricsElasticsearch.elasticsearch.failAllContinue","true");
                        properties.put("metricsElasticsearch.http.timeoutSocket","60000");
                        properties.put("metricsElasticsearch.http.timeoutConnection","40000");
                        properties.put("metricsElasticsearch.http.connectionRequestTimeout","70000");
                        properties.put("metricsElasticsearch.http.maxTotal","200");
                        properties.put("metricsElasticsearch.http.defaultMaxPerRoute","100");
                        return ElasticSearchBoot.boot(properties);

                    }
                });
            }

            @Override
            public void afterStartAction(ImportContext importContext) {
                ClientInterface clientInterface =  ElasticSearchHelper.getConfigRestClientUtil("metricsElasticsearch","indice.xml");
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    clientInterface.dropIndice("vops-loginmodulemetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginmodulemetrics failed:",e);
                }
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    clientInterface.dropIndice("vops-loginusermetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginusermetrics failed:",e);
                }
                //创建Elasticsearch指标表
                clientInterface.createIndiceMapping("vops-loginmodulemetrics","vops-loginmodulemetrics-dsl");
                clientInterface.createIndiceMapping("vops-loginusermetrics","vops-loginusermetrics-dsl");
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
                        .setElasticsearch("metricsElasticsearch")//指定明细Elasticsearch集群数据源名称，bboss可以支持多数据源
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
                bulkProcessorHolder.setObject(bulkProcessor);
            }
        });
        //作业结束后，销毁初始化阶段创建的BulkProcessor，释放资源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {

                bulkProcessorHolder.getObject().shutDown();//作业结束时关闭批处理器

                importContext.destroyResources(new ResourceEnd() {//关闭初始化阶段创建的Elasticsearch数据源，释放资源
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
                    String metricKey = "LoginModuleMetric:"+operModule;
                    metric(metricKey, mapData, new KeyMetricBuilder() {
                        @Override
                        public KeyMetric build() {
                            return new LoginModuleMetric();
                        }

                    });

                    //指标2 按照用户统计操作次数
                    String logUser = (String) data.getData("logOperuser");
                    metricKey = "LoginUserMetric:"+logUser;
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
                        bulkProcessorHolder.getObject().insertData("vops-loginmodulemetrics", esData);//将指标计算结果异步批量写入Elasticsearch表vops-loginmodulemetrics
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
                        bulkProcessorHolder.getObject().insertData("vops-loginusermetrics", esData);//将指标计算结果异步批量写入Elasticsearch表vops-loginusermetrics
                    }

                });

            }
        };

        /**
         * 自定义创建不同指标的MapData,设置BuildMapData接口即可
         */
        keyMetrics.setBuildMapData(new BuildMapData() {
            @Override
            public MapData buildMapData(MetricsData metricsData) {
                BuildMapDataContext buildMapDataContext = metricsData.getBuildMapDataContext();
                CommonRecord record = metricsData.getCommonRecord();
                MapData mapData = new MapData(){
                    /**
                     * 根据指标标识，获取指标的时间统计维度字段，默认返回dataTime字段logOpertime值，不同的指标需要指定不同的时间维度统计字段
                     * 分析处理作业可以覆盖本方法，自定义获取时间维度字段值
                     * @param metricsKey
                     * @return
                     */
                    public Date metricsDataTime(String metricsKey) {
                        if(metricsKey.startsWith("LoginUserMetric:") ) {//根据不同的key获取对应的指标时间字段,LoginUserMetric指标使用collectime时间字段作为时间维度
                           Date time = (Date)record.getData("collectime");
                           return time;
                        }
                        else {
                            return (Date) record.getData("logOpertime");//其他指标使用全局logOpertime字段作为时间维度
                        }
                    }

                };
                mapData.setData(record);
                mapData.setDayFormat(buildMapDataContext.getDayFormat());
                mapData.setHourFormat(buildMapDataContext.getHourFormat());
                mapData.setMinuteFormat(buildMapDataContext.getMinuteFormat());
                mapData.setYearFormat(buildMapDataContext.getYearFormat());
                mapData.setMonthFormat(buildMapDataContext.getMonthFormat());
                mapData.setWeekFormat(buildMapDataContext.getWeekFormat());
                return mapData;
            }
        });


        importBuilder.setDataTimeField("logOpertime");//设置默认指标统计时间维度字段
        importBuilder.addMetrics(keyMetrics);

        //数据输入配置
        FileInputConfig fileInputConfig = new FileInputConfig();
        fileInputConfig.setCharsetEncode("GB2312");
        fileInputConfig.setMaxFilesThreshold(10);
        FileConfig fileConfig = new FileConfig();
//        fileConfig.setFieldSplit(";");//指定日志记录字段分割符
//        //指定字段映射配置
//        fileConfig.addCellMapping(0, "logOperTime");
//
//        fileConfig.addCellMapping(1, "operModule");
//        fileConfig.addCellMapping(2, "logOperuser");

        fileInputConfig.addConfig(fileConfig.setSourcePath("D:/logs/metrics-report")//指定目录
                        .setFileHeadLineRegular("^\\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
                        .setFileFilter(new FileFilter() {
                            @Override
                            public boolean accept(FilterFileInfo fileInfo, FileConfig fileConfig) {
                                //判断是否采集文件数据，返回true标识采集，false 不采集
                                return fileInfo.getFileName().startsWith("metrics-report");
                            }
                        })//指定文件过滤器
                        .addField("tag","elasticsearch")//添加字段tag到记录中
                        .setEnableInode(false).setCloseOlderTime(10*1000L)
                //				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
                //.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
        );
        fileInputConfig.setCleanCompleteFiles(true);//删除已完成文件
        fileInputConfig.setFileLiveTime(30 * 1000L);//已采集完成文件存活时间，超过这个时间的文件就会根据CleanCompleteFiles标记，进行清理操作，单位：毫秒
        fileInputConfig.setRegistLiveTime(60 * 1000L);//已完成文件状态记录有效期，单位：毫秒
        fileInputConfig.setScanOldRegistRecordInterval(30 * 1000L);//扫描过期已完成文件状态记录时间间隔，默认为1天，单位：毫秒

        fileInputConfig.setEnableMeta(true);

        importBuilder.setInputConfig(fileInputConfig);
        //数据输出配置，指定elasticsearch数据源名称
        ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
        elasticsearchOutputConfig.setTargetElasticsearch("metricsElasticsearch");
        //指定索引名称，这里采用的是elasticsearch 7以上的版本进行测试，不需要指定type
        elasticsearchOutputConfig.setIndex("metrics-report");
        //指定索引类型，这里采用的是elasticsearch 7以上的版本进行测试，不需要指定type
        //elasticsearchOutputConfig.setIndexType("idxtype");
        importBuilder.setOutputConfig(elasticsearchOutputConfig);
        //流处理配置结束

		//增量配置开始
		importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("filelogesoldregistrecord_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
		//增量配置结束

		//映射和转换配置开始

		importBuilder.addFieldValue("author","张无忌");



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
                context.addFieldValue("logOperTime",new Date());
                context.addFieldValue("operModule","系统管理");
                context.addFieldValue("logOperuser","admin" );
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
		importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
			@Override
			public void success(TaskCommand<String> taskCommand, String o) {
				logger.info("result:"+o);
			}

			@Override
			public void error(TaskCommand<String> taskCommand, String o) {
				logger.warn("error:"+o);
			}

			@Override
			public void exception(TaskCommand<String> taskCommand, Throwable exception) {
				logger.warn("error:",exception);
			}


		});
		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(10);//设置批量导入线程池工作线程数量
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
