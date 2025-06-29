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
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.excel.ExcelFileConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.jobflow.BaseJobFlowNodeFunction;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.builder.*;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.listener.JobFlowListener;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.file.input.ExcelFileInputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.TimeUtil;
import org.frameworkset.util.concurrent.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *  案例说明：采用bboss jobflow实现Deepseek模型推理对话，通过流程上下文，记录每轮会话的记录
 *
 * @author biaoping.yin
 * @Date 2025/6/11
 */
public class JobFlow2ndDeepseekTest {
    private static Logger logger = LoggerFactory.getLogger(JobFlow2ndDeepseekTest.class);
    private static void initDeepseekService(){
        Map properties = new HashMap();

        //deepseek为的向量模型服务数据源名称
        properties.put("http.poolNames","deepseek");

        properties.put("deepseek.http.hosts","https://api.deepseek.com");///设置向量模型服务地址(这里调用的xinference发布的模型服务),多个地址逗号分隔，可以实现点到点负载和容灾
        properties.put("deepseek.http.httpRequestInterceptors","org.frameworkset.datatran.imp.jobflow.ApiKeyHttpRequestInterceptor");//设置apiKey
        properties.put("deepseek.http.timeoutSocket","60000");
        properties.put("deepseek.http.timeoutConnection","40000");
        properties.put("deepseek.http.connectionRequestTimeout","70000");
        properties.put("deepseek.http.maxTotal","100");
        properties.put("deepseek.http.defaultMaxPerRoute","100");
        //
        HttpRequestProxy.startHttpPools(properties);//启动服务

    }
    public static void main(String[] args){
        initDeepseekService();
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        JobFlowScheduleConfig jobFlowScheduleConfig = new JobFlowScheduleConfig();
        jobFlowScheduleConfig.setExecuteOneTime(true);
        jobFlowBuilder.setJobFlowScheduleConfig(jobFlowScheduleConfig);
        
         
        /**
         * 1.构建第一个任务节点：单任务节点 写诗
         */
        DeepseekJobFlowNodeBuilder jobFlowNodeBuilder = new DeepseekJobFlowNodeBuilder("1", "Deepseek-chat-写诗", new DeepseekJobFlowNodeFunction() {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {

       
                List<DeepseekMessage> deepseekMessageList = new ArrayList<>();
                DeepseekMessage deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("system");
                deepseekMessage.setContent("你是一位唐代诗人.");
                deepseekMessageList.add(deepseekMessage);
                jobFlowNodeExecuteContext.addJobFlowContextData("messages", deepseekMessageList);
                deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("user");
                deepseekMessage.setContent("模仿李白的风格写一首七律.飞机!");
                deepseekMessageList.add(deepseekMessage);
                DeepseekMessages deepseekMessages = new DeepseekMessages();
                deepseekMessages.setMessages(deepseekMessageList);
                deepseekMessages.setModel(model);
                deepseekMessages.setStream(stream);
                deepseekMessages.setMax_tokens(this.max_tokens);
                Map response = HttpRequestProxy.sendJsonBody(this.getDeepseekService(), deepseekMessages, "/chat/completions",Map.class);
//                messages.append(response.choices[0].message)
//                logger.info(response);
                List choices = (List) response.get("choices");
                Map message = (Map) ((Map)choices.get(0)).get("message");
                deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("assistant");
                deepseekMessage.setContent((String)message.get("content"));
                deepseekMessageList.add(deepseekMessage);
                logger.info(deepseekMessage.getContent());
                return response;
        }
 
        }).setDeepseekService("deepseek").setModel("deepseek-chat").setMax_tokens(4096);
         
        /**
         * 2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);

        /**
         * 3.构建第一个任务节点：单任务节点 分析诗
         */
        jobFlowNodeBuilder = new DeepseekJobFlowNodeBuilder("2", "Deepseek-chat-分析诗", new DeepseekJobFlowNodeFunction() {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {
                List<DeepseekMessage> deepseekMessageList = (List<DeepseekMessage>) jobFlowNodeExecuteContext.getJobFlowContextData("messages");
                DeepseekMessage deepseekMessage = new DeepseekMessage();

                deepseekMessage.setRole("user");
                deepseekMessage.setContent("帮忙评估上述诗词的意境");
                deepseekMessageList.add(deepseekMessage);
                DeepseekMessages deepseekMessages = new DeepseekMessages();
                deepseekMessages.setMessages(deepseekMessageList);
                deepseekMessages.setModel(model);
                deepseekMessages.setStream(stream);
                deepseekMessages.setMax_tokens(this.max_tokens);
                Map response = HttpRequestProxy.sendJsonBody(this.getDeepseekService(), deepseekMessages, "/chat/completions", Map.class);
//                messages.append(response.choices[0].message)
//                logger.info(response);
                List choices = (List) response.get("choices");
                Map message = (Map) ((Map) choices.get(0)).get("message");
                deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("assistant");
                deepseekMessage.setContent((String) message.get("content"));
                deepseekMessageList.add(deepseekMessage);
                logger.info(deepseekMessage.getContent());
                return response;
            }

        }).setDeepseekService("deepseek").setModel("deepseek-chat").setMax_tokens(4096);

        /**
         * 4 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);
        
        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
        

    }
}
