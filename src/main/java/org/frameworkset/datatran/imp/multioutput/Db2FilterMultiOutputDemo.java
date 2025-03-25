package org.frameworkset.datatran.imp.multioutput;
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

import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.datatran.imp.metrics.LoginModuleMetric;
import org.frameworkset.datatran.imp.metrics.LoginUserMetric;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.MetricsLogLevel;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.minio.MinioFileConfig;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.plugin.kafka.output.Kafka2OutputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.plugin.multi.output.OutputRecordsFilter;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Description: 将从数据库采集的数据同时输出到Elasticsearch和数据库
 * </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Db2FilterMultiOutputDemo {
	private static final Logger logger = LoggerFactory.getLogger(Db2FilterMultiOutputDemo.class);
	public static void main(String[] args){
		Db2FilterMultiOutputDemo dbdemo = new Db2FilterMultiOutputDemo();
//		dbdemo.fullImportData(  dropIndice);
//		dbdemo.scheduleImportData(dropIndice);
		dbdemo.scheduleImportData();
//		dbdemo.scheduleImportData(dropIndice);
	}

	/**
	 * 将从数据库采集的数据同时输出到Elasticsearch和数据库
	 */
	public void scheduleImportData(){
		ImportBuilder importBuilder = ImportBuilder.newInstance();
        importBuilder.setJobId("Db2FilterMultiOutputDemo");
        /**
         * 设置增量状态ID生成策略，在设置jobId的情况下起作用
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
         * ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用[jobType]+[jobId]+[作业查询语句/文件路径等信息的hashcode]，作为增量id作为增量状态id
         * 默认值ImportIncreamentConfig.STATUSID_POLICY_JOBID_QUERYSTATEMENT
         */
        importBuilder.setStatusIdPolicy(ImportIncreamentConfig.STATUSID_POLICY_JOBID);


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

		DBOutputConfig dbOutputConfig = new DBOutputConfig();
		dbOutputConfig.setDbName("target")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
				.setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(true)//是否使用连接池
				.setSqlFilepath("sql.xml")
				.setInsertSqlName("insertSql_javaName");
		importBuilder.addOutputConfig(dbOutputConfig);

        ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
        elasticsearchOutputConfig
                .addTargetElasticsearch("elasticsearch.serverNames","default")
                .addElasticsearchProperty("default.elasticsearch.rest.hostNames","192.168.137.1:9200")
                .addElasticsearchProperty("default.elasticsearch.showTemplate","false")
                .addElasticsearchProperty("default.elasticUser","elastic")
                .addElasticsearchProperty("default.elasticPassword","changeme")
                .addElasticsearchProperty("default.elasticsearch.failAllContinue","true")
                .addElasticsearchProperty("default.http.timeoutSocket","60000")
                .addElasticsearchProperty("default.http.timeoutConnection","40000")
                .addElasticsearchProperty("default.http.connectionRequestTimeout","70000")
                .addElasticsearchProperty("default.http.maxTotal","200")
                .addElasticsearchProperty("default.http.defaultMaxPerRoute","100")
                .setIndex("dbdemo")
                .setEsIdField("log_id")//设置文档主键，不设置，则自动产生文档id
                .setDebugResponse(false)//设置是否将每次处理的reponse打印到日志文件中，默认false
                .setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
        importBuilder.addOutputConfig(elasticsearchOutputConfig);


        importBuilder.setOutputRecordsFilter((config, records) -> {
            /**
             * 可以根据插件类型过滤记录，亦可以根据id或者name过滤记录
             */
            if(config instanceof ElasticsearchOutputConfig) {
                return records;
            }
            else{
                //最多只返回前两条记录
                List<CommonRecord> newRecords = new ArrayList<>();
                for(int i = 0; i < records.size() ; i ++) {
                    newRecords.add(records.get(i));
                    if(i == 2)
                        break;
                }
                return  newRecords;
            }
        });

        ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_KeyMetircs){
            @Override
            public void builderMetrics(){
                //指标1 按操作模块统计模块操作次数
                addMetricBuilder(new MetricBuilder() {
                    @Override
                    public MetricKey buildMetricKey(MapData mapData){
                        CommonRecord data = (CommonRecord) mapData.getData();
//                        logger.info(SimpleStringUtil.object2json(data.getDatas()));
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
                            public KeyMetric build() {
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
            }

            /**
             * 存储指标计算结果
             * @param metrics
             */
            @Override
            public void persistent(Collection< KeyMetric> metrics) {
                Object value = importContext.getJobContextData("name");
                metrics.forEach(keyMetric->{
                    List<Map> loginmodulekeymetrics = new ArrayList<>();
                    List<Map> loginuserkeymetrics = new ArrayList<>();
                    if(keyMetric instanceof LoginModuleMetric) {
                        LoginModuleMetric testKeyMetric = (LoginModuleMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());

                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("operModule", testKeyMetric.getOperModule());
                        esData.put("count", testKeyMetric.getCount());
                        loginmodulekeymetrics.add(esData);
                    }
                    else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());

                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("logUser", testKeyMetric.getLogUser());
                        esData.put("count", testKeyMetric.getCount());
                        loginuserkeymetrics.add(esData);
                    }
                    ClientInterface clientInterface = ElasticSearchHelper.getRestClientUtil();
                    if(loginmodulekeymetrics.size() > 0)
                        clientInterface.addDocuments("vops-loginmodulekeymetrics",loginmodulekeymetrics);
                    if(loginuserkeymetrics.size() > 0)
                        clientInterface.addDocuments("vops-loginuserkeymetrics",loginuserkeymetrics);
                });

            }
        };

        MetricsOutputConfig metricsOutputConfig = new MetricsOutputConfig();

//        metricsOutputConfig.setDataTimeField("logOpertime");
        metricsOutputConfig.addMetrics(keyMetrics);

        importBuilder.addOutputConfig(metricsOutputConfig);
        importBuilder.setFlushMetricsOnScheduleTaskCompleted(true);

        FileOutputConfig fileOutputConfig = new FileOutputConfig();
        MinioFileConfig minioFileConfig = new MinioFileConfig();
        fileOutputConfig.setMinioFileConfig(minioFileConfig);

        minioFileConfig.setBackupSuccessFiles(true);
        minioFileConfig.setTransferEmptyFiles(false);
        minioFileConfig.setEndpoint("http://172.24.176.18:9000");

        minioFileConfig.setName("miniotest");
        minioFileConfig.setAccessKeyId("O3CBPdUzJICHsMp7pj6h");
        minioFileConfig.setSecretAccesskey("Y6o9piJTjhL6wRQcHeI7fRCyeM2LTSavGcCVx8th");
        minioFileConfig.setConnectTimeout(5000);
        minioFileConfig.setReadTimeout(5000);
        minioFileConfig.setWriteTimeout(5000);
        minioFileConfig.setBucket("etlfiles");
        minioFileConfig.setFailedFileResendInterval(-1);
        minioFileConfig.setSendFileAsyn(true);//异步发送文件
        fileOutputConfig.setMaxFileRecordSize(200);
        fileOutputConfig.setFileDir("c:/workdir/ES2FileMinioDemo");
        fileOutputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {
                 
                String time = taskContext.getTaskStringData("time");
                String _fileSeq = fileSeq+"";
                int t = 6 - _fileSeq.length();
                if(t > 0){
                    String tmp = "";
                    for(int i = 0; i < t; i ++){
                        tmp += "0";
                    }
                    _fileSeq = tmp+_fileSeq;
                }



                return "HN_BOSS_TRADE_"+time +"_" + _fileSeq+".txt";
            }
        });

        fileOutputConfig.setRecordGenerator(new RecordGenerator() {
            @Override
            public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) {
                SerialUtil.object2jsonDisableCloseAndFlush(record.getDatas(),builder);
//				System.out.println(data);

            }
        });
        importBuilder.addOutputConfig(fileOutputConfig);

        ExcelFileOutputConfig excelFileOutputConfig = new ExcelFileOutputConfig();
        minioFileConfig = new MinioFileConfig();
        excelFileOutputConfig.setMinioFileConfig(minioFileConfig);

        minioFileConfig.setBackupSuccessFiles(true);
        minioFileConfig.setTransferEmptyFiles(false);
         

        minioFileConfig.setName("miniotest");
       
        minioFileConfig.setBucket("excelfiles");
        minioFileConfig.setFailedFileResendInterval(-1);
        minioFileConfig.setSendFileAsyn(true);//异步发送文件
        excelFileOutputConfig.setTitle("师大2021年新生医保（2021年）申报名单");
        excelFileOutputConfig.setSheetName("2021年新生医保申报单");

        excelFileOutputConfig.addCellMapping(0,"operModule","操作模块")
                .addCellMapping(1,"author","作者")
                .addCellMapping(2,"logContent","*logContent")
                .addCellMapping(3,"title","*title")

                .addCellMapping(4,"logOpertime","*logOpertime")
                .addCellMapping(5,"logOperuser","*logOperuser")

                .addCellMapping(6,"ipinfo","*ipinfo")
                .addCellMapping(7,"collecttime","collecttime")
                .addCellMapping(8,"subtitle","*子标题");
        excelFileOutputConfig.setFileDir("c:/workdir/excels");//数据生成目录

        excelFileOutputConfig.setExistFileReplace(false);//替换重名文件，如果不替换，就需要在genname方法返回带序号的文件名称
        excelFileOutputConfig.setMaxFileRecordSize(200);
        excelFileOutputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {
                String time = taskContext.getTaskStringData("time");
                return "师大2021年新生医保（2021年）申报名单-合并-"+time+"-"+fileSeq+".xlsx";
            }
        });
        importBuilder.addOutputConfig(excelFileOutputConfig);


        // kafka服务器参数配置
        // kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
        Kafka2OutputConfig kafkaOutputConfig = new Kafka2OutputConfig();
        kafkaOutputConfig.setTopic("db2kafka");
        kafkaOutputConfig.addKafkaProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaOutputConfig.addKafkaProperty("key.serializer","org.apache.kafka.common.serialization.LongSerializer");
        kafkaOutputConfig.addKafkaProperty("compression.type","gzip");
        kafkaOutputConfig.addKafkaProperty("bootstrap.servers","101.131.6.127:9092");
        kafkaOutputConfig.addKafkaProperty("batch.size","10");
//		kafkaOutputConfig.addKafkaProperty("linger.ms","10000");
//		kafkaOutputConfig.addKafkaProperty("buffer.memory","10000");
        kafkaOutputConfig.setKafkaAsynSend(true);
//指定文件中每条记录格式，不指定默认为json格式输出
        kafkaOutputConfig.setRecordGenerator(new RecordGenerator() {
            @Override
            public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) {
                //直接将记录按照json格式输出到文本文件中
                SerialUtil.object2jsonDisableCloseAndFlush(record.getDatas(),//获取记录中的字段数据
                        builder);
                

            }
        });
        importBuilder.addOutputConfig(kafkaOutputConfig);
        
        importBuilder.setMetricsLogLevel(MetricsLogLevel.WARN);
        //在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作
		importBuilder.setImportStartAction(new ImportStartAction() {
			/**
			 * 所有初始化操作完成后，导出数据之前执行的操作
			 * @param importContext
			 */
			@Override
			public void afterStartAction(ImportContext importContext) {
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil().dropIndice("dbdemo");
                } catch (Exception e) {
                    logger.error("Drop indice dbdemo failed:",e);
                }
				try {
					SQLExecutor.deleteWithDBName("target","delete from batchtest");//删目标库target的tablename表中的数据

				} catch (SQLException throwables) {
                    logger.error("delete batchtest data failed:",throwables);
				}
			}
			/**
			 * 初始化之前执行的处理操作，比如后续初始化操作、数据处理过程中依赖的资源初始化
			 * @param importContext
			 */
			@Override
			public void startAction(ImportContext importContext) {
			}


		});
		importBuilder.setBatchSize(100); //可选项,批量导入db的记录数，默认为-1，逐条处理，> 0时批量处理
		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(300000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束
//
//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
		importBuilder.addCallInterceptor(new CallInterceptor() {
			/**
			 * 每次定时执行任务时进行的操作
			 * @param taskContext
			 */
			@Override
			public void preCall(TaskContext taskContext) {
                String formate = "yyyyMMddHHmm";
                //HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
                SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
                String time = dateFormat.format(new Date());
                taskContext.addTaskData("time",time);
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

		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("logdb2db_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型

//
		final AtomicInteger s = new AtomicInteger(0);
		importBuilder.setGeoipDatabase("C:/workdir/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("C:/workdir/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("C:/workdir/geolite2/ip2region.db");
		/**
		 * 重新设置数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}



				context.addFieldValue("author","duoduo");
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");
				context.addFieldValue("collecttime",new Date());//

//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
				context.addIgnoreFieldMapping("subtitle");
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				IpInfo ipInfo = context.getIpInfo("LOG_VISITORIAL");
				if(ipInfo != null)
					context.addFieldValue("ipinfo", SimpleStringUtil.object2json(ipInfo));
				else{
					context.addFieldValue("ipinfo", "");
				}
				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
				Date optime = context.getDateValue("LOG_OPERTIME",dateFormat);
				context.addFieldValue("logOpertime",optime);
				context.addFieldValue("collecttime",new Date());
//				对数据进行格式化
//				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//				Date optime = context.getDateValue("LOG_OPERTIME");
//
//				context.addFieldValue("logOpertime",dateFormat.format(optime));

				/**
				 //关联查询数据,单值查询
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,"test",
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,"test",
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
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

		importBuilder.setUseJavaName(true)
				.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
		importBuilder.setExportResultHandler(new ExportResultHandler() {
			@Override
			public void success(TaskCommand  taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
//				logger.info(taskMetrics.toString());
			}

			@Override
			public void error(TaskCommand  taskCommand, Object result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.error(taskMetrics.toString());
			}

			@Override
			public void exception(TaskCommand  taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.error(taskMetrics.toString());
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
