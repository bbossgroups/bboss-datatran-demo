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

import org.frameworkset.spi.ai.callback.AgentOutput;
import org.frameworkset.spi.ai.flow.AINodeAgent;
import org.frameworkset.spi.ai.flow.AIPlanAgent;
import org.frameworkset.spi.ai.model.ChatAgentMessage;
import org.frameworkset.spi.ai.model.ServerEvent;
import org.frameworkset.spi.ai.store.StoreContext;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.jobflow.schedule.HolidayJobFlowScheduleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  案例说明：采用bboss jobflow实现Deepseek模型推理对话流程，通过流程上下文，记录每轮会话的记录
 *
 * @author biaoping.yin
 * @Date 2025/6/11
 */
public class JobFlow2ndDeepseekScheduleTest {
    private static Logger logger = LoggerFactory.getLogger(JobFlow2ndDeepseekScheduleTest.class);
    private static void initDeepseekService(){
      
        HttpRequestProxy.startHttpPools("application.properties");//启动服务

    }
    public static void main(String[] args){
        //初始化Deepseek服务
        initDeepseekService();
        ChatAgentMessage chatAgentMessage = new ChatAgentMessage();
        chatAgentMessage.setModel("deepseek-chat").setMaas("deepseek");

        //构建流程
        AIPlanAgent planAgent = new AIPlanAgent(new StoreContext().setStoreType(StoreContext.STORE_TYPE_MEMORY).setSessionSize(100));
        planAgent.setAgentName("Deepseek写诗-评价诗词流程")
                .setAgentId("测试id").setAgentMessage(chatAgentMessage);
        
        HolidayJobFlowScheduleConfig jobFlowScheduleConfig = new HolidayJobFlowScheduleConfig();
        jobFlowScheduleConfig.setDelay(1000L);
        jobFlowScheduleConfig.setFixedRate(true);
        jobFlowScheduleConfig.setPeriod(30000L);
        jobFlowScheduleConfig.addCustomHoliday("2026-06-10");
        planAgent.setJobFlowScheduleConfig(jobFlowScheduleConfig);

        planAgent.addAgent(new AINodeAgent("模仿李白的风格写一首七律.飞机!").setSystemPrompt("你是一位唐代诗人.").setAgentId("1").setAgentName("Deepseek写诗-评价诗词流程").setAgentOutput(new AgentOutput() {
            @Override
            public void output(ServerEvent message) {
                logger.info("--------诗歌内容---------\n{}",message.getData());
            }
        }));

        planAgent.addAgent(new AINodeAgent("帮忙评估上述诗词的意境").setAgentId("2").setAgentName("Deepseek-chat-分析诗").setAgentOutput(new AgentOutput() {
            @Override
            public void output(ServerEvent message) {
                logger.info("--------诗歌评价---------\n{}",message.getData());
            }
        }));


        planAgent.chat();
         
        

    }
}
