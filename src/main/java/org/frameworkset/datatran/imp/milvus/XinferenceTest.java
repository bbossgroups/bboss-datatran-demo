package org.frameworkset.datatran.imp.milvus;
/**
 * Copyright 2024 bboss
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

import org.frameworkset.spi.remote.http.HttpRequestProxy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/4
 */
public class XinferenceTest {
    public static void main(String[] args){
        String content = "admin(系统管理员) 登陆[公共开发平台]";
        //初始化向量模型服务
        Map properties = new HashMap();

        //embedding_model为的向量模型服务数据源名称
        properties.put("http.poolNames","embedding_model_xinference,embedding_model_lanchat");

        properties.put("embedding_model_xinference.http.hosts","172.24.176.18:9997");//设置向量模型服务地址，这里调用的xinference发布的模型服务

        properties.put("embedding_model_xinference.http.timeoutSocket","60000");
        properties.put("embedding_model_xinference.http.timeoutConnection","40000");
        properties.put("embedding_model_xinference.http.connectionRequestTimeout","70000");
        properties.put("embedding_model_xinference.http.maxTotal","100");
        properties.put("embedding_model_xinference.http.defaultMaxPerRoute","100");

        properties.put("embedding_model_lanchat.http.hosts","127.0.0.1:7861");//设置向量模型服务地址，这里调用的xinference发布的模型服务

        properties.put("embedding_model_lanchat.http.timeoutSocket","60000");
        properties.put("embedding_model_lanchat.http.timeoutConnection","40000");
        properties.put("embedding_model_lanchat.http.connectionRequestTimeout","70000");
        properties.put("embedding_model_lanchat.http.maxTotal","100");
        properties.put("embedding_model_lanchat.http.defaultMaxPerRoute","100");
        HttpRequestProxy.startHttpPools(properties);

        Map params = new HashMap();
        params.put("text",content);
        //调用的Langchain-Chatchat封装的xinference发布的模型服务，将LOG_CONTENT转换为向量数据---lanchat返回结果
        BaseResponse baseResponse = HttpRequestProxy.sendJsonBody("embedding_model_lanchat",params,"/fqa-py-api/knowledge_base/kb_embedding_textv1",BaseResponse.class);
        if(baseResponse.getCode() == 200){
            float[] embedding = baseResponse.getData();//获取向量数据
        }
        params = new HashMap();
        params.put("input",content);
        params.put("model","custom-bge-large-zh-v1.5");
//                   {"input": ["\\u5411\\u91cf\\u8f6c\\u6362"], "model": "custom-bge-large-zh-v1.5", "encoding_format": "base64"}
//                   String params = "{\"input\": [\"\\u5411\\u91cf\\u8f6c\\u6362\"], \"model\": \"custom-bge-large-zh-v1.5\", \"encoding_format\": \"base64\"}";
//                   Headers({'host': '172.24.176.18:9997', 'accept-encoding': 'gzip, deflate, br', 'connection': 'keep-alive', 
//                   'accept': 'application/json', 'content-type': 'application/json', 'user-agent': 'OpenAI/Python 1.35.13', 
//                   'x-stainless-lang': 'python', 'x-stainless-package-version': '1.35.13', 'x-stainless-os': 'Windows', 
//                   'x-stainless-arch': 'other:amd64', 'x-stainless-runtime': 'CPython', 'x-stainless-runtime-version': '3.10.14', 
//                   'authorization': '[secure]', 'x-stainless-async': 'false', 
//                   'openai-organization': '', 'content-length': '105'})
        //调用向量服务，将LOG_CONTENT转换为向量数据
        String base64 = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));//---与lanchat返回结果不一致
        base64 = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
        String sparams = "{\"input\": [\""+base64+"\"], \"model\": \"custom-bge-large-zh-v1.5\", \"encoding_format\": \"base64\"}";
        String sresult = HttpRequestProxy.sendJsonBody("embedding_model_xinference",sparams,"/v1/embeddings",String.class);//---与lanchat返回结果不一致

        base64 = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.ISO_8859_1));
        String sparams1 = "{\"input\": [\""+base64+"\"], \"model\": \"custom-bge-large-zh-v1.5\", \"encoding_format\": \"base64\"}";
        String sresult1 = HttpRequestProxy.sendJsonBody("embedding_model_xinference",sparams1,"/v1/embeddings",String.class);//---与lanchat返回结果不一致
        //调用的xinference发布的模型服务
        XinferenceResponse result = HttpRequestProxy.sendJsonBody("embedding_model_xinference",params,"/v1/embeddings",XinferenceResponse.class);//-------与lanchat返回结果一致
        float[] embedding = result.getData().get(0).getEmbedding();
        System.out.println();
    }

}
