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

import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.builder.SimpleJobFlowNodeBuilder;

/**
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class SimpleJobFlowNodeBuilderTest extends SimpleJobFlowNodeBuilder {

    /**
     * 串行节点作业配置
     *
     * @param nodeId
     * @param nodeName
     */
    public SimpleJobFlowNodeBuilderTest(String nodeId, String nodeName) {
        super(nodeId, nodeName);
    }

    @Override
    protected JobFlowNodeFunction buildJobFlowNodeFunction() {
        return new JobFlowNodeFunctionTest();
    }
}
