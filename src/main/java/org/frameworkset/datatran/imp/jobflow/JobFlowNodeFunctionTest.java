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
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class JobFlowNodeFunctionTest implements JobFlowNodeFunction {

    private JobFlowNode jobFlowNode;
    private boolean throwError;
    public JobFlowNodeFunctionTest(boolean throwError){
        this.throwError = throwError;
    }
    @Override
    public void init(JobFlowNode jobFlowNode) {
        this.jobFlowNode = jobFlowNode;
    }

    @Override
    public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
//        if(true)
//            throw new RuntimeException("测试异常");
        if(this.throwError) {
            jobFlowNode.nodeComplete(new Exception("测试异常"));//直接完成任务，需要根据实际情况在任务处理完毕后调用节点完成方法
        }
        else{
            jobFlowNode.nodeComplete(null);
        }
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
