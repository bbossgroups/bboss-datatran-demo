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
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.metrics.MetricsLogLevel;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.output.minio.MinioFileConfig;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.plugin.kafka.input.Kafka2InputConfig;
import org.frameworkset.tran.plugin.kafka.output.Kafka2OutputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_JSON;
import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_LONG;

/**
 * <p>Description: 将从数据库采集的数据同时输出到Elasticsearch和数据库
 * </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2MultiOutputDemo {
	private static final Logger logger = LoggerFactory.getLogger(Kafka2MultiOutputDemo.class);
	public static void main(String[] args){
		Kafka2MultiOutputDemo dbdemo = new Kafka2MultiOutputDemo();
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
        importBuilder.setJobId("Db2ESDBdemo");
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
		 * 源kafka相关配置
         * ./kafka-consumer-groups.sh --bootstrap-server <broker_address> --describe --group <consumer_group> --topic <topic_name>
         *     ./kafka-consumer-groups.sh --bootstrap-server 172.24.176.18:9092 --describe --group trandbtest --topic file2kafka
		 */
        Kafka2InputConfig kafka2InputConfig = new Kafka2InputConfig();
        kafka2InputConfig.addKafkaConfig("group.id","file2kafka1") // 消费组ID
                .addKafkaConfig("session.timeout.ms","30000")
                .addKafkaConfig("auto.commit.interval.ms","5000")
                .addKafkaConfig("auto.offset.reset","latest")
//				.addKafkaConfig("bootstrap.servers","192.168.137.133:9093")
                .addKafkaConfig("bootstrap.servers","172.24.176.18:9092")
                .addKafkaConfig("enable.auto.commit","false")
                .addKafkaConfig("max.poll.records","500") // The maximum number of records returned in a single call to poll().
                .setKafkaTopic("file2kafka1") // kafka topic
                .setConsumerThreads(5) // 并行消费线程数，建议与topic partitions数一致
                .setKafkaWorkQueue(10)
                .setKafkaWorkThreads(2)

                .setPollTimeOut(1000) // 从kafka consumer poll(timeout)参数
                .setValueCodec(CODEC_JSON)//"org.apache.kafka.common.serialization.ByteArrayDeserializer"
                .setKeyCodec(CODEC_LONG);//"org.apache.kafka.common.serialization.ByteArrayDeserializer"

        importBuilder.setInputConfig(kafka2InputConfig);

        FileOutputConfig fileOupputConfig = new FileOutputConfig();



        fileOupputConfig.setFileDir("C:\\workdir\\excels");//数据生成目录

        fileOupputConfig.setExistFileReplace(false);//替换重名文件，如果不替换，就需要在genname方法返回带序号的文件名称
        fileOupputConfig.setMaxFileRecordSize(15);
        fileOupputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {
                Date date = taskContext.getJobStartTime();
                String time = DateFormateMeta.format(date,"yyyyMMddHHmmss");
                return "师大2021年新生医保（2021年）申报名单-合并-"+time+"-"+fileSeq+".txt";
            }
        });

        FtpOutConfig ftpOutConfig = new FtpOutConfig();
        ftpOutConfig
                .setFtpIP("172.24.176.18")
                .setFtpPort(22)
                .setFtpUser("wsl")
                .setFtpPassword("123456")
                .setRemoteFileDir("/home/wsl/ftp")
                .setKeepAliveTimeout(100000)
                .setTransferProtocol(FtpConfig.TRANSFER_PROTOCOL_SFTP)
                .setBackupSuccessFiles(true)
                .setTransferEmptyFiles(true)
                .setFailedFileResendInterval(300000)
                .setSendFileAsyn(true)
        ;

        fileOupputConfig.setFtpOutConfig(ftpOutConfig);


        importBuilder.addOutputConfig(fileOupputConfig);

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
                .setIndex("multikafkademo")
                .setEsIdField("logId")//设置文档主键，不设置，则自动产生文档id
                .setDebugResponse(false)//设置是否将每次处理的reponse打印到日志文件中，默认false
                .setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
        importBuilder.addOutputConfig(elasticsearchOutputConfig);
        
        importBuilder.setMetricsLogLevel(MetricsLogLevel.WARN);
        //在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作
		importBuilder.setImportStartAction(new ImportStartAction() {
			/**
			 * 所有初始化操作完成后，导出数据之前执行的操作
			 * @param importContext
			 */
			@Override
			public void afterStartAction(ImportContext importContext) {
                
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

		 
		importBuilder.setGeoipDatabase("C:/workdir/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("C:/workdir/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("C:/workdir/geolite2/ip2region.db");
		/**
		 * 重新设置数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
	

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
