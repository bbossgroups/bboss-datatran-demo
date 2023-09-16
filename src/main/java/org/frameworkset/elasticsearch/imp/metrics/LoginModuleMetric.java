package org.frameworkset.elasticsearch.imp.metrics;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/2/15
 * @author biaoping.yin
 * @version 1.0
 */
public class LoginModuleMetric extends TimeMetric {
    private String operModule ;
    @Override
    public void init(MapData firstData) {
        CommonRecord data = (CommonRecord) firstData.getData();
        //获取用于指标计算处理等的临时数据到记录，不会对临时数据进行持久化处理，
        String name = (String)data.getTempData("name");
        operModule = (String) data.getData("operModule");
        if(operModule == null || operModule.equals("")){
            operModule = "未知模块";
        }
    }

    @Override
    public void incr(MapData data) {
        count ++;
    }

    public String getOperModule() {
        return operModule;
    }
}
