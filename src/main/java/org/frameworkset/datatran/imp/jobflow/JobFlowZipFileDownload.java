package org.frameworkset.datatran.imp.jobflow;
/**
 * Copyright 2025 bboss
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

import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.jobflow.*;
import org.frameworkset.tran.jobflow.builder.DatatranJobFlowNodeBuilder;
import org.frameworkset.tran.jobflow.builder.JobFlowBuilder;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author biaoping.yin
 * @Date 2025/9/21
 */
public class JobFlowZipFileDownload {
    private static Logger logger = LoggerFactory.getLogger(SimpleJobFlowTest.class);
    public static void main(String[] args){
        /**
         * 1.定义工作流以及流程调度策略：流程启动后，延后5秒后开始执行，每隔30秒周期性调度执行
         */
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        JobFlowScheduleConfig jobFlowScheduleConfig = new JobFlowScheduleConfig();
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateHours(new Date(),2));//2小时后开始执行
        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateSeconds(new Date(),5));//5秒后开始执行
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDates(new Date(),10));//10天后结束
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDateMinitues(new Date(),10));//2分钟后结束
        jobFlowScheduleConfig.setPeriod(30000L);
//        jobFlowScheduleConfig.setExecuteOneTime(true);
//        jobFlowScheduleConfig.setExecuteOneTimeSyn(true);
        jobFlowBuilder.setJobFlowScheduleConfig(jobFlowScheduleConfig);

        


        /**
         * 2.构建第一个任务节点：Zip文件下载解压节点
         */
        RemoteFileInputJobFlowNodeBuilder jobFlowNodeBuilder = new RemoteFileInputJobFlowNodeBuilder() ;
        Map downloadedFileRecorder = new ConcurrentHashMap<String,Object>();
        Object o = new Object();
        /**
         * 2.1 设置载情况跟踪记录器
         */
        jobFlowNodeBuilder.setDownloadedFileRecorder(new DownloadedFileRecorder() {
            /**
             * 通过本方法记录下载文件信息，同时亦可以判断文件是否已经下载过，如果已经下载过则返回false，忽略下载，否则返回true允许下载
             * @param downloadFileMetrics
             * @param jobFlowNodeExecuteContext
             * @return
             */
            @Override
            
            public boolean recordBeforeDownload(DownloadFileMetrics downloadFileMetrics, JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
                //如果文件已经下载过，则返回false，忽略下载,一般会持久化保存到数据库中
                if(downloadedFileRecorder.get(downloadFileMetrics.getRemoteFilePath()) != null)
                    return false;
                return true;
            }

            /**
             * 文件下载完毕或者错误时调用本方法记录已完成或者下载失败文件信息，可以自行存储下载文件信息，以便下次下载时判断文件是否已经下载过             *
             * @param downloadFileMetrics
             * @param jobFlowNodeExecuteContext
             * @param exception 
             */
            @Override
            public void recordAfterDownload(DownloadFileMetrics downloadFileMetrics, JobFlowNodeExecuteContext jobFlowNodeExecuteContext, Throwable exception) {
                //如果文件下载解压成功，则记录下载信息
                if(exception == null) {
                    //获取从当前压缩文件中解压的文件数量并判断是否大于0，则将解压文件数量保存到流程上下文数据中，用于作为数据采集作业节点的触发条件（只有当前解压文件数量大于0时，才触发下一个任务节点）
                    if(downloadFileMetrics.getFiles() > 0)
                        jobFlowNodeExecuteContext.addJobFlowContextData("unzipFiles",downloadFileMetrics.getFiles());
                    downloadedFileRecorder.put(downloadFileMetrics.getRemoteFilePath(), o);
                }
            }
        });
        /**
         * 2.2 设置Zip下载远程参数
         */
        jobFlowNodeBuilder.setBuildDownloadConfigFunction(jobFlowNodeExecuteContext -> {
            FtpConfig ftpConfig = new FtpConfig().setFtpIP("172.24.176.18").setFtpPort(22)
                    .setFtpUser("wsl").setFtpPassword("123456").setDownloadWorkThreads(4).setTransferProtocol(FtpConfig.TRANSFER_PROTOCOL_SFTP)
                    .setRemoteFileDir("/mnt/c/data/1000").setSocketTimeout(600000L)
                    .setConnectTimeout(600000L)
                    .setUnzip(true)
                    .setUnzipDir("c:/data/unzipfile")//解压目录
                    .setZipFilePassward("123456").setDeleteZipFileAfterUnzip(false)
                    .setSourcePath("c:/data/zipfile");//下载目录
            //向后续数据采集作业传递数据文件存放目录
            jobFlowNodeExecuteContext.addJobFlowContextData("csvfilepath",ftpConfig.getUnzipDir());
            DownloadfileConfig downloadfileConfig = new DownloadfileConfig();
            downloadfileConfig
                    .setFtpConfig(ftpConfig)
                    .setScanChild(true)
                    .setFileNameRegular(".*\\.zip");
            return downloadfileConfig;
        });
        

        


        /**
         * 3.将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);


        /**
         * 4.构建第二个任务节点：数据采集作业节点
         */
        DatatranJobFlowNodeBuilder datatranJobFlowNodeBuilder = new DatatranJobFlowNodeBuilder();
        

        /**
         * 4.1设置数据采集作业构建函数
         */
        datatranJobFlowNodeBuilder.setImportBuilderFunction(jobFlowNodeExecuteContext -> {
            CSVUserBehaviorImport csvUserBehaviorImport = new CSVUserBehaviorImport();
            return csvUserBehaviorImport.buildImportBuilder(jobFlowNodeExecuteContext);
        });
        //4.2 为数据采集作业任务节点添加触发器，当上个节点解压文件数量大于0时，则触发数据采集作业，否则不触发
        NodeTrigger parrelnewNodeTrigger = new NodeTrigger();
        parrelnewNodeTrigger.setTriggerScriptAPI(new TriggerScriptAPI() {
            @Override
            public boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception {
                Object unzipFiles = nodeTriggerContext.getJobFlowExecuteContext().getContextData("unzipFiles");
                //当上个节点解压文件数量大于0时，则触发数据采集作业，否则不触发
                if(unzipFiles == null || ((Integer)unzipFiles) <= 0){
                    return false;
                }
                else {
                    return true;
                }
            }
        });
        datatranJobFlowNodeBuilder.setNodeTrigger(parrelnewNodeTrigger);
        /**
         * 5 将第二个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(datatranJobFlowNodeBuilder);


        /**
         * 6 构建并启动工作流
         */
        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
//        
//        jobFlow.stop();
////
//        jobFlow.pause();
////        
//        jobFlow.consume();


    }
}
