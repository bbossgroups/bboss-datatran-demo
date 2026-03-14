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
import org.frameworkset.util.TimeUtil;
import org.frameworkset.util.concurrent.IntegerCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class SimpleConditionJobFlowTest {
    private static Logger logger = LoggerFactory.getLogger(SimpleConditionJobFlowTest.class);
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
        JobFlowNodeBuilder jobFlowNodeBuilder = new CallableJobFlowNodeBuilder("1", "SimpleNode") {
            @Override
            public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {
                JobFlowNodeFunctionTest jobFlowNodeFunctionTest = new JobFlowNodeFunctionTest(false);
                jobFlowNodeFunctionTest.init(jobFlowNodeExecuteContext.getJobFlowNode());
                return jobFlowNodeFunctionTest.call(jobFlowNodeExecuteContext);
            }
        } ;

        JobFlowNodeBuilder conditionJobFlowNodeBuilder = jobFlowNodeBuilder;

        IntegerCount integerCount = new IntegerCount();
        NodeTrigger nodeTrigger = new NodeTrigger();
        nodeTrigger.setTriggerScriptAPI(new TriggerScriptAPI() {
            @Override
            public boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception {
                if(integerCount.getCountUnSynchronized() > 229) {
                    logger.info("最多允许执行230次：迭代结束");
                    return false;
                }
                
                if(integerCount.getCountUnSynchronized() == 228){
                    logger.info("228");
                }
                return true;
            }
        });


        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setNodeTrigger(nodeTrigger);
        /**
         * 1.2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
        
        

        /**
         * 2.构建第二个任务节点：并行任务节点
         */
        ParrelJobFlowNodeBuilder parrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder("2","ParrelJobFlowNode");
        parrelJobFlowNodeBuilder.addJobFlowNodeListener(new JobFlowNodeListener() {
            @Override
            public void beforeExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
                integerCount.increament();
            }

            @Override
            public void afterExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, Throwable throwable) {
                logger.info(SimpleStringUtil.object2json(jobFlowNodeExecuteContext.getJobFlowNodeStaticContext()));
            }

            @Override
            public void afterEnd(JobFlowNode jobFlowNode) {

            }
        });
        NodeTrigger parrelnewNodeTrigger = new NodeTrigger();
        parrelnewNodeTrigger.setTriggerScriptAPI(new TriggerScriptAPI() {
            @Override
            public boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception {
                return true;
            }
        });
        parrelJobFlowNodeBuilder.setNodeTrigger(parrelnewNodeTrigger);
        /**
         * 2.1 为第二个并行任务节点添加第一个带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(
                new CommonJobFlowNodeBuilder("ParrelJobFlowNode-DatatranJobFlowNode-2-1","ParrelJobFlowNode-DatatranJobFlowNode-2",new JobFlowNodeFunctionTest(false,true)).setNodeTrigger(nodeTrigger));
        /**
         * 2.2 为第二个并行任务节点添加第二个不带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-DatatranJobFlowNode-2-2","ParrelJobFlowNode-DatatranJobFlowNode-2",new JobFlowNodeFunctionTest(false)) );
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
        subnode.setNodeTrigger(NodeTriggerBuilder.buildNodeTrigger(new TriggerScriptAPI() {
            @Override
            public boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception {
                //从串行复合节点comJobFlowNodeBuilder中获取分支循环执行次数）
                IntegerCount executeTimes = (IntegerCount) nodeTriggerContext.getContainerContextData("executeTimes");
                if(executeTimes == null){
                    return true;                     
                }
                if(executeTimes.getCountUnSynchronized() >= 2) {
                    return false;
                }
                return true;
            }
        }));
        subnode.addJobFlowNodeListener(new JobFlowNodeListener() {
            @Override
            public void beforeExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
                
            }

            @Override
            public void afterExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, Throwable throwable) {
                //设置节点执行次数往节点父节点（串行复合节点comJobFlowNodeBuilder中添加循环执行测试）
                IntegerCount executeTimes = (IntegerCount) jobFlowNodeExecuteContext.getContainerJobFlowNodeContextData("executeTimes");
                if(executeTimes == null){
                    executeTimes = new IntegerCount();
                   
                    jobFlowNodeExecuteContext.addContainerJobFlowNodeContextData("executeTimes",executeTimes);
                }
                executeTimes.increament();
            }

            @Override
            public void afterEnd(JobFlowNode jobFlowNode) {

            }
        });
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(subnode);
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-2-3-2","SequenceJobFlowNode-SequenceJobFlowNode",new JobFlowNodeFunctionTest(false)).setNodeTrigger(nodeTrigger) );
        //串行分支中增加条件分支节点，指向第一个子节点subnode，形成一个局部循环链路，需要通过触发器设置循环结束条件，否则会进入无限循环
        comJobFlowNodeBuilder.addConditionJobFlowNodeBuilder(subnode, true);
        
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(comJobFlowNodeBuilder);

        ParrelJobFlowNodeBuilder subParrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder("ParrelJobFlowNode-2-4","ParrelJobFlowNode");
        subParrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-2-4-1","ParrelJobFlowNode-SequenceJobFlowNode",new JobFlowNodeFunctionTest(false)) );
        subParrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-2-4-2","ParrelJobFlowNode-SequenceJobFlowNode",new JobFlowNodeFunctionTest(false)) );
        /**
         * 2.4 为第二个并行任务节点添加第三个并行行复杂流程子任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(subParrelJobFlowNodeBuilder);

        /**
         * 2.5 将第二个节点添加到工作流中
         */
        jobFlowBuilder.addJobFlowNodeBuilder(parrelJobFlowNodeBuilder);

        /**
         * 3.构建第三个任务节点：单任务节点
         */
        jobFlowNodeBuilder = new CommonJobFlowNodeBuilder("3","SimpleNode",new JobFlowNodeFunctionTest(false));
        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setNodeTrigger(nodeTrigger);

        /**
         * 1.2 将第三个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);

        jobFlowBuilder.addConditionJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("4","SimpleNode",new JobFlowNodeFunctionTest(false)),true);

        jobFlowBuilder.addConditionJobFlowNodeBuilder(conditionJobFlowNodeBuilder);
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
