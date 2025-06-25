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

import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 函数接口案例
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class JobFlowNodeFunctionTest implements JobFlowNodeFunction {

    private JobFlowNode jobFlowNode;
    private boolean throwError;
    public JobFlowNodeFunctionTest(boolean throwError){
        this.throwError = throwError;
    }

    /**
     * 函数初始化，在一个JobFlow实例中，每个节点的函数接口只会被创建一次，因此初始化方法只会被调用一次
     * @param jobFlowNode
     */
    @Override
    public void init(JobFlowNode jobFlowNode) {
        this.jobFlowNode = jobFlowNode;
    }

    /**
     * 具体任务业务逻辑实现，处理完业务逻辑后，务必调用jobFlowNode.nodeComplete方法
     * @param jobFlowNodeExecuteContext
     * @return
     */
    @Override
    public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
        Throwable exception = null;
        try {
            //获取流程上下文参数functionParam的值，参数有效期为本次执行过程中有效，执行完毕后直接被清理
            Object flowParam = jobFlowNodeExecuteContext.getJobFlowExecuteContext().getContextData("flowParam","defaultValue");
            //如果节点包含在串行或者并行复合节点中，可以获取串行或者并行复合节点上下文中的参数，参数有效期为本次执行过程中有效，执行完毕后直接被清理
            if(jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext() != null) {
                Object containerNodeParam = jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext().getContextData("containerNodeParam");
            }
            //直接获取节点执行上下文中添加参数，参数有效期为本次执行过程中有效，执行完毕后直接被清理
            Object nodeParam =  jobFlowNodeExecuteContext.getContextData("nodeParam");
            //此处编写业务逻辑
            //.......
            if(jobFlowNodeExecuteContext.assertStopped().isTrue()){
                return null;
            }
            if(this.throwError) {
                //模拟异常发生
                exception = new Exception("测试异常");
            }
            //更新或添加流程上下文参数functionParam的值，向流程流程后续节点传递参数，参数有效期为本次执行过程中有效，执行完毕后直接被清理
            jobFlowNodeExecuteContext.getJobFlowExecuteContext().addContextData("flowParam","paramValue");
            //如果节点包含在串行或者并行复合节点中，可以向串行或者并行复合节点上下文中添加参数，以便在复合节点的子节点间共享参数，参数有效期为本次执行过程中有效，执行完毕后直接被清理
            if(jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext() != null) {
                jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext().addContextData("containerNodeParam", "paramValue");
            }
            //直接在节点执行上下文中添加参数，参数有效期为本次执行过程中有效，执行完毕后直接被清理
            jobFlowNodeExecuteContext.addContextData("nodeParam", "paramValue");

            

        }        
        finally {
            //方法执行完毕后，务必调用jobFlowNode.nodeComplete方法，如果方法执行过程中产生异常，则作为参数传递给complete方法
            jobFlowNode.nodeComplete(exception);
        }
        //此处可以返回一个值，目前无用
        return null;
    }

    @Override
    public void reset() {

    }

    @Override
    public void release() {

    }
 

    @Override
    public void stop() {

    }
}
