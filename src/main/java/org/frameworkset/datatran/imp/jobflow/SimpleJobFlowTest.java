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

import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.builder.*;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.util.TimeUtil;

import java.util.Date;

/**
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class SimpleJobFlowTest {
    public static void main(String[] args){
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        JobFlowScheduleConfig jobFlowScheduleConfig = new JobFlowScheduleConfig();
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateHours(new Date(),2));//2小时后开始执行
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateMinitues(new Date(),1));//1分钟后开始执行
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDates(new Date(),10));//10天后结束
        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDateMinitues(new Date(),10));//2分钟后结束
        jobFlowScheduleConfig.setPeriod(1000000L);
        jobFlowScheduleConfig.setExecuteOneTime(true);
        jobFlowBuilder.setJobFlowScheduleConfig(jobFlowScheduleConfig);

        /**
         * 作为测试用例，所有的作业工作流流程节点共用一个作业定义
         */
//        ImportBuilder importBuilder = build();
        /**
         * 1.构建第一个任务节点：单任务节点
         */
        JobFlowNodeBuilder jobFlowNodeBuilder = new CommonJobFlowNodeBuilder("1", "DatatranJobFlowNode",new JobFlowNodeFunctionTest()) ;
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
                .append("//前序节点执行异常结束，则忽略当前节点执行\r\n")
                .append("if(staticContext != null && staticContext.getExecuteException() != null)")
                .append("    return false;")
                .append("else{")
                .append("    return true;")
                .append("}").toString();
        nodeTrigger.setTriggerScript(script);
//        
//        nodeTrigger.setTriggerScriptAPI(new TriggerScriptAPI() {
//            @Override
//            public boolean evalTriggerScript(NodeTriggerContext nodeTriggerContext) throws Exception {
//                StaticContext staticContext = nodeTriggerContext.getPreJobFlowStaticContext();
//                if(staticContext != null && staticContext.getExecuteException() != null)
//                    return false;
//                else{
//                    return true;
//                }
//            }
//        });
        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setNodeTrigger(nodeTrigger);
        /**
         * 1.2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);

        /**
         * 2.构建第二个任务节点：并行任务节点
         */
        ParrelJobFlowNodeBuilder parrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder("2","ParrelJobFlowNode");
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
                new CommonJobFlowNodeBuilder("ParrelJobFlowNode-DatatranJobFlowNode-2","ParrelJobFlowNode-DatatranJobFlowNode-2-1",new JobFlowNodeFunctionTest()).setNodeTrigger(nodeTrigger));
        /**
         * 2.2 为第二个并行任务节点添加第二个不带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-DatatranJobFlowNode-2","ParrelJobFlowNode-DatatranJobFlowNode-2-2",new JobFlowNodeFunctionTest()) );
        /**
         * 2.3 为第二个并行任务节点添加第三个串行复杂流程子任务
         */
        SequenceJobFlowNodeBuilder comJobFlowNodeBuilder = new SequenceJobFlowNodeBuilder("ParrelJobFlowNode-2-3","SequenceJobFlowNode");
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("SequenceJobFlowNode-SequenceJobFlowNode","ParrelJobFlowNode-2-3-1",new JobFlowNodeFunctionTest()) );
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("SequenceJobFlowNode-SequenceJobFlowNode","ParrelJobFlowNode-2-3-2",new JobFlowNodeFunctionTest()).setNodeTrigger(nodeTrigger) );

        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(comJobFlowNodeBuilder);

        ParrelJobFlowNodeBuilder subParrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder("ParrelJobFlowNode-2-4","ParrelJobFlowNode");
        subParrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-SequenceJobFlowNode","ParrelJobFlowNode-2-4-1",new JobFlowNodeFunctionTest()) );
        subParrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-SequenceJobFlowNode","ParrelJobFlowNode-2-4-2",new JobFlowNodeFunctionTest()) );
        /**
         * 2.4 为第二个并行任务节点添加第三个并行行复杂流程子任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(subParrelJobFlowNodeBuilder);

        /**
         * 2.5 将第二个节点添加到工作流中
         */
        jobFlowBuilder.addJobFlowNode(parrelJobFlowNodeBuilder);

        /**
         * 3.构建第三个任务节点：单任务节点
         */
        jobFlowNodeBuilder = new CommonJobFlowNodeBuilder("3","DatatranJobFlowNode",new JobFlowNodeFunctionTest());
        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setNodeTrigger(nodeTrigger);

        /**
         * 1.2 将第三个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);

        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
//        
        jobFlow.stop();
//
        jobFlow.pause();
//        
        jobFlow.consume();


    }
}
