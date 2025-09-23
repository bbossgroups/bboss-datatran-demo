package org.frameworkset.datatran.imp.jobflow;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.frameworkset.bulk.*;
import org.frameworkset.datatran.imp.file.UserBehaviorMetric;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.csv.CSVFileConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.plugin.custom.output.CustomOutPutContext;
import org.frameworkset.tran.plugin.custom.output.CustomOutPutV1;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.file.input.CSVFileInputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.record.FieldMappingManager;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.frameworkset.util.beans.ObjectHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CSVUserBehaviorImport {

    private static Logger logger = LoggerFactory.getLogger(CSVUserBehaviorImport.class);

    private static final String JOB_ID = "JOB_USER_BEHAVIOR_JOB_ID";
    private static final String JOB_NAME = "JOB_USER_BEHAVIOR_JOB_NAME";

    private String ftpIp;
    private int ftpPort;
    private String ftpUser;
    private String ftpPassword;
    private String ftpRemoteFileDir;
    private String fileDir;
    private String prefix;
    private int threadCount;
    private int queue;

    private int maxFilesThreshold;



    private void initFtpConfigParam() {
        PropertiesContainer propertiesContainer = org.frameworkset.spi.assemble.PropertiesUtil.getPropertiesContainer();
        String job = "userBehaviorJob";
        ftpIp = propertiesContainer.getSystemEnvProperty(job+".ftp.ip");
        ftpPort = propertiesContainer.getIntSystemEnvProperty(job+".ftp.port",22);
        ftpUser = propertiesContainer.getSystemEnvProperty(job+".ftp.user");
        ftpPassword = propertiesContainer.getSystemEnvProperty(job+".ftp.password");
        ftpRemoteFileDir = propertiesContainer.getSystemEnvProperty(job+".ftpRemoteFileDir");
        fileDir = propertiesContainer.getSystemEnvProperty(job+".fileDir");
        prefix = propertiesContainer.getSystemEnvProperty(job+".file.prefix");
        threadCount = propertiesContainer.getIntSystemEnvProperty(job+".threadCount",5);
        queue = propertiesContainer.getIntSystemEnvProperty(job+".queue",10);
        maxFilesThreshold = propertiesContainer.getIntSystemEnvProperty(job+".maxFilesThreshold", 10);
    }

 
    public ImportBuilder buildImportBuilder(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
        logger.info("build UserBehaviorJob started.");
        //构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
        ObjectHolder<CommonBulkProcessor> objectHolder = new ObjectHolder<CommonBulkProcessor>();
        this.initFtpConfigParam();
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setJobId(JOB_ID).setJobName(JOB_NAME);
        importBuilder.setBatchSize(1000)//设置批量入库的记录数
                .setFetchSize(1000);//设置按批读取文件行数
        //设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
        importBuilder.setFlushInterval(10000l);
        CSVFileInputConfig config = new CSVFileInputConfig();
        config.setDisableScanNewFiles( true);
        config.setDisableScanNewFilesCheckpoint(false);
        config.setMaxFilesThreshold(maxFilesThreshold);
        /**
         * 备份采集完成文件
         * true 备份
         * false 不备份
         */
        config.setBackupSuccessFiles(true);
        /**
         * 备份文件目录
         */
        config.setBackupSuccessFileDir("C:\\data\\cvs\\backup");
        /**
         * 备份文件清理线程执行时间间隔，单位：毫秒
         * 默认每隔10秒执行一次
         */
        config.setBackupSuccessFileInterval(20000l);
        /**
         * 备份文件保留时长，单位：秒
         * 默认保留7天
         */
//        config.setBackupSuccessFileLiveTime( 10 * 60l);
       
        CSVFileConfig csvFileConfig = new CSVFileConfig();
        csvFileConfig.setFileFilter(new FileFilter() {//指定ftp文件筛选规则
                    @Override
                    public boolean accept(FilterFileInfo fileInfo, //Ftp文件名称
                                          FileConfig fileConfig) {
                        String name = fileInfo.getFileName();
                        //判断是否采集文件数据，返回true标识采集，false 不采集，business_fans_mobile_20250815_QL.csv
                        boolean nameMatch = name.startsWith("gio_kd_to_chnl_public_platform");
                        if(nameMatch){

                            /**
                             * 采集1分钟之前生成的FTP文件,本地未采集完的文件继续采集
                             */
                            Object fileObject = fileInfo.getFileObject();
                            if(fileObject instanceof RemoteResourceInfo) {
                                RemoteResourceInfo remoteResourceInfo = (RemoteResourceInfo) fileObject;
                                long mtime = remoteResourceInfo.getAttributes().getMtime()*1000;
                                long interval = System.currentTimeMillis() - mtime;
                                if(interval > 100000){
                                    return true;
                                }
                                else{
                                    return false;
                                }
                            }
                            else{
                                return true;
                            }
                        } else {
                            return false;
                        }
                    }
                })
                .setMaxCellIndexMatchesFailedPolicy(FieldMappingManager.MAX_CELL_INDEX_MATCHES_FAILED_POLICY_WARN_USENULLVALUE)
                .setSkipHeaderLines(1)
                .setSourcePath((String)jobFlowNodeExecuteContext.getJobFlowContextData("csvfilepath"));//从流程执行上下文中获取csv文件目录
        config.addConfig(csvFileConfig);
        config.setEnableMeta(true);
//		config.setJsondata(true);
        importBuilder.setInputConfig(config);


        ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_KeyTimeMetircs) {
            @Override
            public void map(MapData mapData) {
                CommonRecord data = (CommonRecord) mapData.getData();
                //可以添加多个指标
                //指标1 用户访问次数统计
                StringBuilder metricKey = new StringBuilder();
                metricKey.append((String) data.getData("USER"));
                metric(metricKey.toString(), mapData, new KeyMetricBuilder() {
                    @Override
                    public KeyMetric build() {
                        return new UserBehaviorMetric();
                    }
                });
            }

            /**
             * 存储指标计算结果
             * @param metrics
             */
            @Override
            public void persistent(Collection<KeyMetric> metrics) {
                metrics.forEach(keyMetric -> {
                    if (keyMetric instanceof UserBehaviorMetric) {
                        UserBehaviorMetric metric = (UserBehaviorMetric) keyMetric;
                        objectHolder.getObject().insertData(metric);
                    }
                });
            }
        };
        // key metrics中包含两个segment(S0,S1)
        keyMetrics.setSegmentBoundSize(20000000);
        keyMetrics.setTimeWindows(100); // 100秒
        keyMetrics.setDataTimeField("EVENT_TIME");//设置指标统计时间维度字段
        keyMetrics.setTimeWindowType(MetricsConfig.TIME_WINDOW_TYPE_DAY);
        keyMetrics.init();

        CustomOutputConfig customOutputConfig =  new CustomOutputConfig();
        customOutputConfig.setCustomOutPutV1(new CustomOutPutV1(){
            /**
             * 自定义输出数据方法
             *
             * @param customOutPutContext 封装需要处理的数据和其他作业上下文信息
             */
            @Override
            public void handleData(CustomOutPutContext customOutPutContext) {
                customOutPutContext.getDatas().forEach(data -> { 
                    logger.info(SimpleStringUtil.object2json( data.getDatas()));
                });
            }
 
        });
        importBuilder.setOutputConfig(customOutputConfig);

        importBuilder.addMetrics(keyMetrics);
        importBuilder.setFlushMetricsOnScheduleTaskCompleted(true);
        importBuilder.setUseDefaultMapData(false);
        this.setImportStartAction(importBuilder, objectHolder);
        this.setImportEndAction(importBuilder, objectHolder);
        //字段映射
//        csvFileConfig.setSkipHeaderLines(200000);//跳过200000行数据
        csvFileConfig.addCellMapping(0, "EVENT_KEY");
        csvFileConfig.addCellMapping(1, "EVENT_TIME");
        csvFileConfig.addCellMapping(2, "USER");
        csvFileConfig.addCellMapping(3, "ATTRIBUTES");
        csvFileConfig.addCellMapping(4, "REFERRER_DOMAIN");
        csvFileConfig.addCellMapping(5, "COUNTRY_CODE");
        csvFileConfig.addCellMapping(6, "COUNTRY_NAME");
        csvFileConfig.addCellMapping(7, "REGION");
        csvFileConfig.addCellMapping(8, "CITY");
        csvFileConfig.addCellMapping(9, "BROWSER_VERSION");
        csvFileConfig.addCellMapping(10, "OS_VERSION");
        csvFileConfig.addCellMapping(11, "DEVICE_BRAND");
        csvFileConfig.addCellMapping(12, "DEVICE_MODEL");
        csvFileConfig.addCellMapping(13, "RESOLUTION");
        csvFileConfig.addCellMapping(14, "IP");
        csvFileConfig.addCellMapping(15, "USER_AGENT");

         
        
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {
                 

                //$page,2025-07-22 16:25:11.581000+08:00,5e53ff48-8d30-43fe-aa7a-1edcc771b46e,"{'$path': '/ech/h5/kd-gift/index.html#/', '$title': '移动宽带报装', '$query': 'channelId=P00000109981&yx=1341974001&DrainageChannel=JTAPPPLUSZHJTBAN&WT.ac_id=kdxz_190604_O_JTAPPPLUSZHJTBAN&recoempltel=OPRCITY2010280004'}",,CN,中国,广东,广州,Chrome Mobile WebView 138.0.7204,Android 12,Vivo,Vivo V2055A,800*360,14.19.193.210,"Mozilla/5.0 (Linux; Android 12; V2055A Build/SP1A.210812.003; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/138.0.7204.157 Mobile Safari/537.36 leadeon/11.9.5/CMCCIT"
//                String[] messageArr = message.split(",");
//                String[] messageArr = CSV_PARSER.parseLine(message);
                // 替换原 CSV_PARSER 使用方式
                  
                String mobile = context.getStringValue("USER");
                // 列字段不导入
                if ("user".equals(mobile)) {
                    logger.warn("列字段: {}", mobile);
                    context.setDrop(true);
                    return;
                }
                // 无效号码不导入
                if (mobile != null && mobile.contains("-")) {
                    logger.warn("无效号码: {}", mobile);
                    context.setDrop(true);
                    return;
                }
                String eventKey = context.getStringValue("EVENT_KEY");
                if("xcxsearchEventClick".equals(eventKey)) { //小程序搜索，号码需要特殊处理
                    mobile = new String(Base64.getDecoder().decode(mobile), StandardCharsets.UTF_8);
//                    AES aes = SecureUtil.aes(aesKey.getBytes(StandardCharsets.UTF_8));
//                    mobile = Base64.getEncoder().encodeToString(aes.encrypt(mobile));
                }
//                List<Map> queryListWithDBName = targetMobileconfigSQLExecutor.queryListWithDBName(Map.class, dbName, "queryTargetMobileByMobile", mobile);
//                if (queryListWithDBName == null || queryListWithDBName.size()==0) {
//                    context.setDrop(true);
//                    return;
//                }
                /**
                 * EVENT_KEY,EVENT_TIME,USER,ATTRIBUTES,REFERRER_DOMAIN,COUNTRY_CODE,COUNTRY_NAME,REGION,CITY,BROWSER_VERSION,OS_VERSION,DEVICE_BRAND,
                 * DEVICE_MODEL,RESOLUTION,IP,USER_AGENT
                 */
                // 使用自定义格式化器解析时间字符串
                String text = context.getStringValue("EVENT_TIME");;
                DateTimeFormatter formatter = null;
                DateFormat hourFormateMeta = null;
                if(text != null && text.length() > 25) {
                    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX");
                    hourFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyy-MM-dd HH:mm:ss.SSSSSS").toDateFormat();
                } else {
                    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
                    hourFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyy-MM-dd HH:mm:ss").toDateFormat();
                }
                OffsetDateTime dateTime = OffsetDateTime.parse(text, formatter);
                LocalDateTime localDateTime = dateTime.toLocalDateTime();
                Date eventTime = hourFormateMeta.parse(Timestamp.valueOf(localDateTime).toString());
//                context.addFieldValue("ACCEPT_TIME", eventTime);
//                java.util.Date date = java.sql.Timestamp.valueOf(localDateTime);
//                String eventTime2 = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
//                String now = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
                Date now = new Date();
                context.addFieldValue("COLLECT_TIME", now);
                context.addFieldValue("CREATE_TIME", now);
                context.addFieldValue("UPDATE_TIME", now);
                context.addFieldValue("EVENT_TIME", eventTime);
                context.addFieldValue("USER", mobile);
//                context.addFieldValue("ATTRIBUTES", messageArr[3]);
//                context.addFieldValue("REFERRER_DOMAIN", messageArr[4]);
//                context.addFieldValue("COUNTRY_CODE", messageArr[5]);
//                context.addFieldValue("COUNTRY_NAME", messageArr[6]);
//                context.addFieldValue("REGION", messageArr[7]);
//                context.addFieldValue("CITY", messageArr[8]);
//                context.addFieldValue("BROWSER_VERSION", messageArr[9]);
//                context.addFieldValue("OS_VERSION", messageArr[10]);
//                context.addFieldValue("DEVICE_BRAND", messageArr[11]);
//                context.addFieldValue("DEVICE_MODEL", messageArr[12]);
//                context.addFieldValue("RESOLUTION", messageArr[13]);
//                context.addFieldValue("IP", messageArr[14]);
//                context.addFieldValue("USER_AGENT", messageArr[15]);
                String filePath = (String)context.getMetaValue("filePath");
                context.addFieldValue("filePath",filePath);
                context.addIgnoreFieldMapping("@message");
                context.addIgnoreFieldMapping("@timestamp");
                context.addIgnoreFieldMapping("@filemeta");
            }
        });

//        this.setCallInterceptor(importBuilder);
        this.setExportResultHandler(importBuilder);

//		//设置任务执行拦截器结束，可以添加多个
        //增量配置开始 只适用于数据库采集
//        importBuilder.setLastValueColumn("MOBILE");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
		//importBuilder.setDateLastValueColumn("EVENT_TIME");//手动指定日期增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
        //importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
        //setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
        importBuilder.setLastValueStorePath("C:/data/csv/"+JOB_ID+"_csvimport");//todo 推荐绝对路径 记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
        importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
        // 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
        //增量配置结束

        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(queue);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(threadCount);//设置批量导入线程池工作线程数量 10
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
        importBuilder.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
         
        logger.info("build UserBehaviorJob end");
        return importBuilder;
    }

    private void setBulkAction(ObjectHolder<CommonBulkProcessor> objectHolder) {

        //定义BulkProcessor批处理组件构建器
        CommonBulkProcessorBuilder bulkProcessorBuilder = new CommonBulkProcessorBuilder();
        bulkProcessorBuilder.setBlockedWaitTimeout(-1);//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止
        bulkProcessorBuilder.setBulkSizes(1000);//按批处理数据记录数
        bulkProcessorBuilder.setFlushInterval(5000);//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理
        bulkProcessorBuilder.setWarnMultsRejects(1000);//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
        bulkProcessorBuilder.setWorkThreads(5);//bulk处理工作线程数
        bulkProcessorBuilder.setWorkThreadQueue(10);//bulk处理工作线程池缓冲队列大小
        bulkProcessorBuilder.setBulkProcessorName(JOB_ID);//工作线程名称，实际名称为BulkProcessorName-+线程编号
        bulkProcessorBuilder.setBulkRejectMessage(JOB_ID);//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
        bulkProcessorBuilder.addBulkInterceptor(new CommonBulkInterceptor() {// 添加异步处理结果回调函数
            /**
             * 执行前回调方法
             * @param bulkCommand
             */
            @Override
            public void beforeBulk(CommonBulkCommand bulkCommand) {
            }

            /**
             * 执行成功回调方法
             * @param bulkCommand
             * @param result
             */
            @Override
            public void afterBulk(CommonBulkCommand bulkCommand, BulkResult result) {
                if (logger.isDebugEnabled()) {
                    logger.debug("", result.getResult());
                }
            }

            /**
             * 执行异常回调方法
             * @param bulkCommand
             * @param exception
             */
            @Override
            public void exceptionBulk(CommonBulkCommand bulkCommand, Throwable exception) {
                if (logger.isErrorEnabled()) {
                    logger.error("exceptionBulk", exception);
                }
            }

            /**
             * 执行过程中部分数据有问题回调方法
             * @param bulkCommand
             * @param result
             */
            @Override
            public void errorBulk(CommonBulkCommand bulkCommand, BulkResult result) {
                if (logger.isWarnEnabled()) {
                    logger.warn("", result);
                }
            }
        });
 

        //添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
        /**
         * 设置执行数据批处理接口，实现对数据的异步批处理功能逻辑
         */
        bulkProcessorBuilder.setBulkAction(new BulkAction() {
            @Override
            public BulkResult execute(CommonBulkCommand command) {
                List<CommonBulkData> bulkDataList = command.getBatchBulkDatas();//拿出要进行批处理操作的数据
                List<UserBehaviorMetric> taskMetricList4Insert = new ArrayList<>();
                List<UserBehaviorMetric> taskMetricList4Update = new ArrayList<>();
                Object objData = null;
                
                BulkResult bulkResult = new BulkResult();//构建批处理操作结果对象
                try {
                    
                    // todo 打印耗时
                } catch (Exception e) {
                    //如果执行出错，则将错误信息设置到结果中
                    logger.error("", e);
                    bulkResult.setError(true);
                    bulkResult.setErrorInfo(e.getMessage());
                }
                return bulkResult;
            }
        });

        /**
         * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
         */
        final CommonBulkProcessor bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
        objectHolder.setObject(bulkProcessor);
    }

    private void setImportStartAction(ImportBuilder importBuilder, ObjectHolder<CommonBulkProcessor> objectHolder) {
        importBuilder.setImportStartAction(new ImportStartAction() {
            @Override
            public void startAction(ImportContext importContext) {
                setBulkAction(objectHolder);
            }

            @Override
            public void afterStartAction(ImportContext importContext) {
            }
        });
    }

    private void setImportEndAction(ImportBuilder importBuilder, ObjectHolder<CommonBulkProcessor> objectHolder) {
        //作业结束后销毁初始化阶段自定义的http数据源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {
                //作业结束时关闭批处理器
                objectHolder.getObject().shutDown();
            }
        });
    }

    private void setCallInterceptor(ImportBuilder importBuilder) {
        //设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
        importBuilder.addCallInterceptor(new CallInterceptor() {
            @Override
            public void preCall(TaskContext taskContext) {
            }

            @Override
            public void afterCall(TaskContext taskContext) {
            }

            @Override
            public void throwException(TaskContext taskContext, Throwable e) {
            }
        });
        //设置任务执行拦截器结束，可以添加多个
    }

    private void setExportResultHandler(ImportBuilder importBuilder) {
        importBuilder.setExportResultHandler(new ExportResultHandler< String>() {
            @Override
            public void success(TaskCommand< String> taskCommand, String result) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                if(logger.isDebugEnabled()) {
                    logger.debug(taskMetrics.toString());
                    logger.debug(result);
                }
            }

            @Override
            public void error(TaskCommand<String> taskCommand, String result) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                if(logger.isWarnEnabled()) {
//                    logger.warn(taskMetrics.toString());
                    logger.warn("------------------------------------------------------------------------------------#############");
                    logger.warn(String.valueOf(taskCommand.getDatas()));
                    logger.warn("------------------------------------------------------------------------------------#############");
                    logger.warn(result);
                }
            }

            @Override
            public void exception(TaskCommand< String> taskCommand, Throwable exception) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                if(logger.isErrorEnabled()) {
                    logger.error(taskMetrics.toString(), exception);
                }
            }
        });
      
    }
}
