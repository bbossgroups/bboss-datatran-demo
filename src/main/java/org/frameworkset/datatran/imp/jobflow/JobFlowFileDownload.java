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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.jobflow.*;
import org.frameworkset.tran.jobflow.builder.*;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.listener.JobFlowListener;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author biaoping.yin
 * @Date 2025/9/21
 */
public class JobFlowFileDownload {
    private static Logger logger = LoggerFactory.getLogger(SimpleJobFlowTest.class);
    public static void main(String[] args){
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        JobFlowScheduleConfig jobFlowScheduleConfig = new JobFlowScheduleConfig();
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateHours(new Date(),2));//2小时后开始执行
        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateSeconds(new Date(),5));//1分钟后开始执行
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDates(new Date(),10));//10天后结束
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDateMinitues(new Date(),10));//2分钟后结束
        jobFlowScheduleConfig.setPeriod(30000L);
//        jobFlowScheduleConfig.setExecuteOneTime(true);
//        jobFlowScheduleConfig.setExecuteOneTimeSyn(true);
        jobFlowBuilder.setJobFlowScheduleConfig(jobFlowScheduleConfig);

        jobFlowBuilder.addJobFlowListener(new JobFlowListener() {
            @Override
            public void beforeStart(JobFlow jobFlow) {

            }

            @Override
            public void beforeExecute(JobFlowExecuteContext jobFlowExecuteContext) {

            }

            @Override
            public void afterExecute(JobFlowExecuteContext jobFlowExecuteContext, Throwable throwable) {
                logger.info(SimpleStringUtil.object2json(jobFlowExecuteContext.getJobFlowMetrics()));
                logger.info(SimpleStringUtil.object2json(jobFlowExecuteContext.getJobFlowStaticContext()));

            }

            @Override
            public void afterEnd(JobFlow jobFlow) {

            }
        });

        /**
         * 作为测试用例，所有的作业工作流流程节点共用一个作业定义
         */
//        ImportBuilder importBuilder = build();
        /**
         * 1.构建第一个任务节点：单任务节点
         */
        RemoteFileInputJobFlowNodeBuilder jobFlowNodeBuilder = new RemoteFileInputJobFlowNodeBuilder() ;
        FtpConfig ftpConfig = new FtpConfig().setFtpIP("172.24.176.18").setFtpPort(22)
                .setFtpUser("wsl").setFtpPassword("123456").setDownloadWorkThreads(4).setTransferProtocol(FtpConfig.TRANSFER_PROTOCOL_SFTP)
                .setRemoteFileDir("/mnt/c/data/1000").setSocketTimeout(600000L)
                .setConnectTimeout(600000L).setSourcePath("c:/data/csvlocal");
        DownloadfileConfig downloadfileConfig = new DownloadfileConfig();
        downloadfileConfig
                .setFtpConfig(ftpConfig)                
                .setScanChild(true)
                .setFileNameRegular(".*\\.txt")
                .setJobFileFilter(new JobFileFilter() {

                    /**
                     * 判断是否采集文件数据，返回true标识采集，false 不采集
                     *
                     * @param fileInfo
                     * @param jobFlowNodeExecuteContext
                     * @return
                     */
                    @Override
                    public boolean accept(FilterFileInfo fileInfo, JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
                        //在次判断文件是否需要重新下载，如果文件已经下载过了，则不需要下载，返回false
                        return true;
                    }
                });
        jobFlowNodeBuilder.setDownloadfileConfig(downloadfileConfig);
        jobFlowNodeBuilder.setAutoNodeComplete(true);
        NodeTrigger nodeTrigger = new NodeTrigger();
        /**
         * boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception
         */
//        nodeTrigger.setTriggerScript("return 0 < 1;");
        String script = new StringBuilder()
                .append("[import]")
                .append("//导入脚本中需要引用的java类\r\n")
                .append(" //import org.frameworkset.tran.jobflow.context.StaticContext; ")
                .append("[/import]")
                .append("StaticContext staticContext = nodeTriggerContext.getPreJobFlowStaticContext();")
                .append("logger.info(\"前序节点执行异常结束，则忽略当前节点执行\");")
                .append("//前序节点执行异常结束，则忽略当前节点执行\r\n")
                .append("if(staticContext != null && staticContext.getExecuteException() != null)")
                .append("    return false;")
                .append("else{")
                .append("    return true;")
                .append("}").toString();
        nodeTrigger.setTriggerScript(script);


        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setNodeTrigger(nodeTrigger);
        /**
         * 1.2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);

      

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
