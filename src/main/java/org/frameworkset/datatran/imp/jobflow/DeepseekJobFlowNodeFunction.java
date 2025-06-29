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

import org.frameworkset.tran.jobflow.BaseJobFlowNodeFunction;

/**
 * @author biaoping.yin
 * @Date 2025/6/29
 */
public abstract class DeepseekJobFlowNodeFunction extends BaseJobFlowNodeFunction {
    protected String model;
    protected String deepseekService;
    protected String helpfulAssistant;
    protected boolean stream;
    protected int max_tokens;

    public int getMax_tokens() {
        return max_tokens;
    }

    public void setMax_tokens(int max_tokens) {
        this.max_tokens = max_tokens;
    }


    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getDeepseekService() {
        return deepseekService;
    }

    public void setDeepseekService(String deepseekService) {
        this.deepseekService = deepseekService;
    }

    public String getHelpfulAssistant() {
        return helpfulAssistant;
    }

    public void setHelpfulAssistant(String helpfulAssistant) {
        this.helpfulAssistant = helpfulAssistant;
    }

    public boolean isStream() {
        return stream;
    }

    public void setStream(boolean stream) {
        this.stream = stream;
    }

 
}
