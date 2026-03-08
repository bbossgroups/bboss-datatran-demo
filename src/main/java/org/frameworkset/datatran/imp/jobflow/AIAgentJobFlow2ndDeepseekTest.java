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

import org.frameworkset.spi.ai.AIAgent;
import org.frameworkset.spi.ai.model.ChatAgentMessage;
import org.frameworkset.spi.ai.model.FunctionToolDefine;
import org.frameworkset.spi.ai.model.ServerEvent;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.builder.CallableJobFlowNodeBuilder;
import org.frameworkset.tran.jobflow.builder.JobFlowBuilder;
import org.frameworkset.tran.jobflow.builder.SimpleJobFlowNodeBuilder;
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
public class AIAgentJobFlow2ndDeepseekTest {
    private static Logger logger = LoggerFactory.getLogger(AIAgentJobFlow2ndDeepseekTest.class);
    private static void initDeepseekService(){
        Map properties = new HashMap();

        //deepseek为的Deepseek服务数据源名称
        properties.put("http.poolNames","deepseek,tool");

        properties.put("deepseek.http.hosts","https://api.deepseek.com");///设置Deepseek服务地址
        properties.put("deepseek.http.apiKeyId","sk-36710e392e244584b3f7fc6a7423bd31");//设置apiKey
        properties.put("deepseek.http.timeoutSocket","60000");
        properties.put("deepseek.http.timeoutConnection","40000");
        properties.put("deepseek.http.connectionRequestTimeout","70000");
        properties.put("deepseek.http.maxTotal","200");
        properties.put("deepseek.http.defaultMaxPerRoute","100");
        properties.put("deepseek.http.modelType","deepseek");
        
        
        properties.put("tool.http.hosts","127.0.0.1:8080");///设置tool服务地址
        properties.put("tool.http.apiKeyId","17689048891086XsDsJVgwiQcmKhOdh23DX4NT");//设置apiKey
        properties.put("tool.http.timeoutSocket","60000");
        properties.put("tool.http.timeoutConnection","40000");
        properties.put("tool.http.connectionRequestTimeout","70000");
        properties.put("tool.http.maxTotal","200");
        properties.put("tool.http.defaultMaxPerRoute","100");
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
        SimpleJobFlowNodeBuilder jobFlowNodeBuilder = new CallableJobFlowNodeBuilder("1", "Deepseek-chat-写诗") {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {

                List<Map<String, Object>> session = new ArrayList<>();
                ChatAgentMessage chatAgentMessage = new ChatAgentMessage()
                        .setSystemPrompt("你是一位唐代诗人.")
                        .setPrompt("模仿李白的风格写一首七律.飞机!")
                        .setSessionSize(50)
                        .setSessionMemory(session)
                        .setModel("deepseek-chat")
                        .setMaxTokens(4096);

                AIAgent aiAgent = new AIAgent();
                ServerEvent serverEvent = aiAgent.chat("deepseek", chatAgentMessage);
//                chatAgentMessage.addAssistantSessionMessage(serverEvent.getData());
                logger.info(serverEvent.getData());
                //将通话记录添加到工作流上下文中，保存Deepseek通话记录
                jobFlowNodeExecuteContext.addJobFlowContextData("chatAgentMessage", chatAgentMessage);
                jobFlowNodeExecuteContext.addJobFlowContextData("shi", serverEvent.getData());
                return serverEvent;
            }
                
        };
      
         
        /**
         * 2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);

        /**
         * 3.构建第二个任务节点：单任务节点 分析诗
         */
        jobFlowNodeBuilder = new CallableJobFlowNodeBuilder("2", "Deepseek-chat-分析诗") {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {
                //从工作流上下文中，获取Deepseek历史通话记录
                ChatAgentMessage chatAgentMessage = (ChatAgentMessage) jobFlowNodeExecuteContext.getJobFlowContextData("chatAgentMessage");
                //将第二个问题添加到工作流上下文中，保存Deepseek通话记录

                chatAgentMessage.setPrompt((String) jobFlowNodeExecuteContext.getJobFlowContextData("shi"));

                AIAgent aiAgent = new AIAgent();
                ServerEvent serverEvent = aiAgent.chat("deepseek", chatAgentMessage);
//                chatAgentMessage.addAssistantSessionMessage(serverEvent.getMessage());
                logger.info(serverEvent.getData());
//                jobFlowNodeExecuteContext.addJobFlowContextData("shi-fenxi", serverEvent.getData());
                return serverEvent;


            }
        };

        /**
         * 4 将第二个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);

        /**
         * 5.构建第三个任务节点：单任务节点 调用工具查询杭州天气
         */
        jobFlowNodeBuilder = new CallableJobFlowNodeBuilder("3", "Deepseek-chat-天气查询") {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {
                //从工作流上下文中，获取Deepseek历史通话记录
                ChatAgentMessage chatAgentMessage = (ChatAgentMessage) jobFlowNodeExecuteContext.getJobFlowContextData("chatAgentMessage");
                chatAgentMessage.setPrompt("查询杭州天气，并根据天气给出穿衣、饮食以及出行建议");
                //用户查询杭州天气


                FunctionToolDefine functionToolDefine = new FunctionToolDefine();
//        functionToolDefine.setType("function");
                functionToolDefine.funtionName2ndDescription("weather_info_query","天气查询服务，根据城市查询当地温度和天气信息")
//                            .putParametersType("object")
                        .requiredParameters("location")
                        .addSubParameter("params","location","string","城市或者地州, 例如：上海市")
                        .setFunctionCall(new ToolFunctionCall() );
//                        .addMapParameter("params","subuser","string","城市或者地州, 例如：上海市")
//                        .addArrayParameter("params","subuser","string","城市或者地州, 例如：上海市")


                chatAgentMessage.registTool(functionToolDefine);
                AIAgent aiAgent = new AIAgent();
                ServerEvent serverEvent = aiAgent.chat("deepseek", chatAgentMessage);
                logger.info(serverEvent.getData());
                return serverEvent;
            }
        };

        /**
         * 4 将第工具调用节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
        
        //构建和运行与Deepseek通话流程
        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
        

    }
}
