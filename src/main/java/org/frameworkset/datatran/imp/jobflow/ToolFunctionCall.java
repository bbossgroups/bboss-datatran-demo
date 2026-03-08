package org.frameworkset.datatran.imp.jobflow;
/**
 * Copyright 2026 bboss
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

import org.frameworkset.spi.ai.model.FunctionCall;
import org.frameworkset.spi.ai.model.FunctionCallException;
import org.frameworkset.spi.ai.model.FunctionTool;
import org.frameworkset.spi.remote.http.HttpRequestProxy;

import java.util.List;
import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/2/15
 */
public class ToolFunctionCall implements FunctionCall {
    private String toolDatasource = "tool";
    
    public ToolFunctionCall(String toolDatasource) {
        this.toolDatasource = toolDatasource;
    }
    public ToolFunctionCall( ) {
    }
 

    @Override
    public Object call(FunctionTool functionTool) throws FunctionCallException {
        Map response = HttpRequestProxy.sendJsonBody(toolDatasource,functionTool.getArguments(),  "/openapi/"+functionTool.getFunctionName() + ".api", Map.class);
        List data = (List)response.get("data");
        if(data == null || data.size() == 0){
            return "没有找到对应数据";
        }
        return data;
//        return "20.22℃";
    }
}
