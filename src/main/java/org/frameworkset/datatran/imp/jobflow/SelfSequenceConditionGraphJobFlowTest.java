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
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.NodeTriggerBuilder;
import org.frameworkset.tran.jobflow.builder.*;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.listener.JobFlowListener;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.util.concurrent.IntegerCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class SelfSequenceConditionGraphJobFlowTest {
    private static Logger logger = LoggerFactory.getLogger(SelfSequenceConditionGraphJobFlowTest.class);
    public static void main(String[] args){
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        JobFlowScheduleConfig jobFlowScheduleConfig = new JobFlowScheduleConfig();
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateHours(new Date(),2));//2小时后开始执行
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateMinitues(new Date(),1));//1分钟后开始执行
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDates(new Date(),10));//10天后结束
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDateMinitues(new Date(),10));//2分钟后结束
//        jobFlowScheduleConfig.setPeriod(1000000L);
        jobFlowScheduleConfig.setExecuteOneTime(true);
        jobFlowScheduleConfig.setExecuteOneTimeSyn(true);
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
        /**
         * 2.3 为第二个并行任务节点添加第三个串行复杂流程子任务
         */
        SequenceJobFlowNodeBuilder comJobFlowNodeBuilder = new SequenceJobFlowNodeBuilder("ParrelJobFlowNode-2-3","SequenceJobFlowNode");
        comJobFlowNodeBuilder.addJobFlowNodeListener(new JobFlowNodeListener() {
            @Override
            public void beforeExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {

            }

            @Override
            public void afterExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, Throwable throwable) {
                logger.info(SimpleStringUtil.object2json(jobFlowNodeExecuteContext.getJobFlowNodeStaticContext()));
            }

            @Override
            public void afterEnd(JobFlowNode jobFlowNode) {

            }
        });
        CommonJobFlowNodeBuilder subnode = new CommonJobFlowNodeBuilder("ParrelJobFlowNode-2-3-1","SequenceJobFlowNode-SequenceJobFlowNode",new JobFlowNodeFunctionTest(false));
        
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(subnode);
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-2-3-2","SequenceJobFlowNode-SequenceJobFlowNode",new JobFlowNodeFunctionTest(false)) );
      


        
        NodeTrigger nodeTrigger = new NodeTrigger();
        nodeTrigger.setTriggerScriptAPI(new TriggerScriptAPI() {
            @Override
            public boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception {
                IntegerCount integerCount = (IntegerCount) nodeTriggerContext.getFlowContextData("integerCount");
                if(integerCount.getCountUnSynchronized() > 229) {
                    logger.info("最多允许执行230次：迭代结束");
                    return false;
                }               
                
                return true;
            }
        });


      
        /**
         * 1.1 将节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(comJobFlowNodeBuilder);
        /**
         * 1.2 构建自循环的条件有向循环图
         */
        jobFlowBuilder.addConditionJobFlowNodeBuilder(comJobFlowNodeBuilder,nodeTrigger);

      
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
