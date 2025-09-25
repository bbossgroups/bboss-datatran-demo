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
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.jobflow.DownloadfileConfig;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.RemoteFileInputJobFlowNodeBuilder;
import org.frameworkset.tran.jobflow.builder.JobFlowBuilder;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * ftp远程文件归档测试
 * @author biaoping.yin
 * @Date 2025/9/21
 */
public class JobFlowOSSFileLifecycleClean {
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
         * 2.构建第一个任务节点：远程文件归档节点
         */
        RemoteFileInputJobFlowNodeBuilder jobFlowNodeBuilder = new RemoteFileInputJobFlowNodeBuilder() ;
        
        /**
         * 2.2 设置OSS文件归档远程参数
         */
        jobFlowNodeBuilder.setBuildDownloadConfigFunction(jobFlowNodeExecuteContext -> {
            //指定OSS服务器参数以及归档的远程目录
            OSSFileInputConfig ossFileInputConfig = new OSSFileInputConfig()
                    .setName("miniotest")
                    .setAccessKeyId("N3XNZFqSZfpthypuoOzL")
                    .setSecretAccesskey("2hkDSEll1Z7oYVfhr0uLEam7r0M4UWT8akEBqO97").setRegion("east-r-a1")
                    .setEndpoint("http://172.24.176.18:9000")
                    .setDownloadWorkThreads(4)
                    .setBucket("zipfile")//指定需归档的OSS Bucket目录，定期归档其下面的过期文件
                    .setSocketTimeout(600000L)
                    .setConnectTimeout(600000L)
                    ;
            DownloadfileConfig downloadfileConfig = new DownloadfileConfig();
            downloadfileConfig
                    .setOssFileInputConfig(ossFileInputConfig)
                    .setScanChild(true)
                    .setFileLiveTime(1 * 24 * 60 * 60 * 1000L)//设置归档文件保存时间，超过7天则归档
                    .setLifecycle(true)//设置归档标记为true
                    .setFileNameRegular(".*\\.zip")//可以指定归档的文件名称正则，匹配的文件才会被归档
                    ;
            return downloadfileConfig;
        });    

        /**
         * 3.将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);

 
        /**
         * 4 构建并启动工作流
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
