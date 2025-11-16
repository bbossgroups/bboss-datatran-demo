package org.frameworkset.datatran.imp.jobflow;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.bulk.CommonBulkProcessor;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.custom.output.CustomOutPutContext;
import org.frameworkset.tran.plugin.custom.output.CustomOutPutV1;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.schedule.FileScanAssertStopBarrier;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.util.beans.ObjectHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParrelTxtUserBehaviorImport {

    private static Logger logger = LoggerFactory.getLogger(ParrelTxtUserBehaviorImport.class);

    private static final String JOB_ID = "JOB_TXT_USER_BEHAVIOR_JOB_ID";
    private static final String JOB_NAME = "JOB_TXT_USER_BEHAVIOR_JOB_NAME";

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
        FileInputConfig fileInputConfig = new FileInputConfig();
        fileInputConfig.setDisableScanNewFiles( true);
        fileInputConfig.setDisableScanNewFilesCheckpoint(false);
        fileInputConfig.setMaxFilesThreshold(maxFilesThreshold);
        
        fileInputConfig.setAssertStopBarrier(new FileScanAssertStopBarrier(jobFlowNodeExecuteContext) {

            @Override
            protected boolean canStop() {
                Boolean downloadNodeComplete = (Boolean)jobFlowNodeExecuteContext.getContainerJobFlowNodeContextData("downloadNodeComplete");
                if(downloadNodeComplete == null){
                    return true;
                }
                else{
                    if(downloadNodeComplete){
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            }
        });
        /**
         * 备份采集完成文件
         * true 备份
         * false 不备份
         */
        fileInputConfig.setBackupSuccessFiles(true);
        /**
         * 备份文件目录
         */
        fileInputConfig.setBackupSuccessFileDir("C:\\data\\cvs\\backup");
        /**
         * 备份文件清理线程执行时间间隔，单位：毫秒
         * 默认每隔10秒执行一次
         */
        fileInputConfig.setBackupSuccessFileInterval(20000l);
        /**
         * 备份文件保留时长，单位：秒
         * 默认保留7天
         */
//        config.setBackupSuccessFileLiveTime( 10 * 60l);
       
        FileConfig fileConfig = new FileConfig();
        fileConfig.setFileFilter(new FileFilter() {//指定文件筛选规则
                    @Override
                    public boolean accept(FilterFileInfo fileInfo, //文件信息
                                          FileConfig fileConfig) {
                        String name = fileInfo.getFileName();
                        return true;
                    }
                }).setSkipHeaderLines(1)
                .setCloseOlderTime(1000L)//setIgnoreOlderTime
                .setSourcePath((String)jobFlowNodeExecuteContext.getContainerJobFlowNodeContextData("csvfilepath"));//从并行任务节点（当前节点的父节点）执行上下文中获取解压数据文件目录
        fileInputConfig.addConfig(fileConfig);
        fileInputConfig.setEnableMeta(true);
//		config.setJsondata(true);
        importBuilder.setInputConfig(fileInputConfig);



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

        importBuilder.setUseDefaultMapData(false);
         
        
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {
                 

//                String[] messageArr = message.split(",");
//                String[] messageArr = CSV_PARSER.parseLine(message);
                // 替换原 CSV_PARSER 使用方式
                  
                
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
