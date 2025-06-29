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
 * @Date 2025/6/29
 */
public class DeepseekJobFlowNodeBuilder extends SimpleJobFlowNodeBuilder {
    private String model;
    private String deepseekService;
    private String helpfulAssistant;
    private boolean stream;
    private int max_tokens;
    private DeepseekJobFlowNodeFunction deepseekJobFlowNodeFunction;

    /**
     * 串行节点作业配置
     *
     * @param nodeId
     * @param nodeName
     */
    public DeepseekJobFlowNodeBuilder(String nodeId, String nodeName,DeepseekJobFlowNodeFunction deepseekJobFlowNodeFunction) {
        super(nodeId, nodeName);
        this.deepseekJobFlowNodeFunction = deepseekJobFlowNodeFunction;
        this.setAutoNodeComplete(true);
        
    }

    @Override
    protected JobFlowNodeFunction buildJobFlowNodeFunction() {
        deepseekJobFlowNodeFunction.setDeepseekService(deepseekService);
        deepseekJobFlowNodeFunction.setModel(model);
        deepseekJobFlowNodeFunction.setStream(stream);
        deepseekJobFlowNodeFunction.setHelpfulAssistant(helpfulAssistant);
        deepseekJobFlowNodeFunction.setMax_tokens(max_tokens);
        return deepseekJobFlowNodeFunction;
    }

    public String getModel() {
        return model;
    }

    public DeepseekJobFlowNodeBuilder setModel(String model) {
        this.model = model;
        return this;
    }

    public String getDeepseekService() {
        return deepseekService;
    }

    public DeepseekJobFlowNodeBuilder setDeepseekService(String deepseekService) {
        this.deepseekService = deepseekService;
        return this;
    }

    public String getHelpfulAssistant() {
        return helpfulAssistant;
    }

    public DeepseekJobFlowNodeBuilder setHelpfulAssistant(String helpfulAssistant) {
        this.helpfulAssistant = helpfulAssistant;
        return this;
    }

    public boolean isStream() {
        return stream;
    }

    public DeepseekJobFlowNodeBuilder setStream(boolean stream) {
        this.stream = stream;
        return this;
    }

    public int getMax_tokens() {
        return max_tokens;
    }

    public DeepseekJobFlowNodeBuilder setMax_tokens(int max_tokens) {
        this.max_tokens = max_tokens;
        return this;
    }
}
