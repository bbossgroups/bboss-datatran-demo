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

import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.builder.JobFlowBuilder;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  案例说明：采用bboss jobflow实现Deepseek模型推理对话流程，通过流程上下文，记录每轮会话的记录
 *
 * @author biaoping.yin
 * @Date 2025/6/11
 */
public class JobFlow2ndDeepseekTest {
    private static Logger logger = LoggerFactory.getLogger(JobFlow2ndDeepseekTest.class);
    private static void initDeepseekService(){
        Map properties = new HashMap();

        //deepseek为的Deepseek服务数据源名称
        properties.put("http.poolNames","deepseek");

        properties.put("deepseek.http.hosts","https://api.deepseek.com");///设置Deepseek服务地址
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
        //初始化Deepseek服务
        initDeepseekService();
        //构建流程
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("Deepseek写诗-评价诗词流程")
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
                //将通话记录添加到工作流上下文中，保存Deepseek通话记录
                jobFlowNodeExecuteContext.addJobFlowContextData("messages", deepseekMessageList);
                
                deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("user");
                deepseekMessage.setContent("模仿李白的风格写一首七律.飞机!");
                //将问题添加到工作流上下文中，保存Deepseek通话记录
                deepseekMessageList.add(deepseekMessage);
                
                DeepseekMessages deepseekMessages = new DeepseekMessages();
                deepseekMessages.setMessages(deepseekMessageList);
                deepseekMessages.setModel(model);
                deepseekMessages.setStream(stream);
                deepseekMessages.setMax_tokens(this.max_tokens);
                //调用Deepseek 对话api提问
                Map response = HttpRequestProxy.sendJsonBody(this.getDeepseekService(), deepseekMessages, "/chat/completions",Map.class);
                List choices = (List) response.get("choices");
                Map message = (Map) ((Map)choices.get(0)).get("message");
                deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("assistant");
                deepseekMessage.setContent((String)message.get("content"));
                //将问题答案添加到工作流上下文中，保存Deepseek通话记录
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
         * 3.构建第二个任务节点：单任务节点 分析诗
         */
        jobFlowNodeBuilder = new DeepseekJobFlowNodeBuilder("2", "Deepseek-chat-分析诗", new DeepseekJobFlowNodeFunction() {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {
                //从工作流上下文中，获取Deepseek历史通话记录
                List<DeepseekMessage> deepseekMessageList = (List<DeepseekMessage>) jobFlowNodeExecuteContext.getJobFlowContextData("messages");
                //将第二个问题添加到工作流上下文中，保存Deepseek通话记录
                DeepseekMessage deepseekMessage = new DeepseekMessage();

                deepseekMessage.setRole("user");
                deepseekMessage.setContent("帮忙评估上述诗词的意境");
                deepseekMessageList.add(deepseekMessage);
                DeepseekMessages deepseekMessages = new DeepseekMessages();
                deepseekMessages.setMessages(deepseekMessageList);
                deepseekMessages.setModel(model);
                deepseekMessages.setStream(stream);
                deepseekMessages.setMax_tokens(this.max_tokens);
                //调用Deepseek 对话api提问
                Map response = HttpRequestProxy.sendJsonBody(this.getDeepseekService(), deepseekMessages, "/chat/completions", Map.class);
                List choices = (List) response.get("choices");
                Map message = (Map) ((Map) choices.get(0)).get("message");
                deepseekMessage = new DeepseekMessage();
                deepseekMessage.setRole("assistant");
                deepseekMessage.setContent((String) message.get("content"));
                //将第二个问题答案添加到工作流上下文中，保存Deepseek通话记录
                deepseekMessageList.add(deepseekMessage);
                logger.info(deepseekMessage.getContent());
                return response;
            }

        }).setDeepseekService("deepseek").setModel("deepseek-chat").setMax_tokens(4096);

        /**
         * 4 将第二个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);
        
        //构建和运行与Deepseek通话流程
        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
        

    }
}
