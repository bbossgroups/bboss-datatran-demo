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

import com.google.common.collect.Lists;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.frameworkset.nosql.milvus.MilvusConfig;
import org.frameworkset.nosql.milvus.MilvusFunction;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.spi.remote.http.HttpRequestProxy;

import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/4
 */
public class MilvusTest {
    public static void search(String[] args){

        /**
         * 参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
         */
        //1. 初始化milvus数据源chan_fqa，用来操作向量数据库，一个milvus数据源只需要定义一次即可，后续通过名称chan_fqa反复引用，多线程安全
        // 可以通过以下方法定义多个Milvus数据源，只要name不同即可，通过名称引用对应的数据源
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName("chan_fqa");//数据源名称
        milvusConfig.setDbName("ucr_chan_fqa");//Milvus数据库名称
        milvusConfig.setUri("http://172.24.176.18:19530");//Milvus数据库地址
        milvusConfig.setToken("");//认证token：root:xxxx

        MilvusHelper.init(milvusConfig);//启动初始化Milvus数据源
        
       
        
        //2. 初始化Xinference向量模型服务embedding_model，一个服务组只需要定义一次即可，后续通过名称embedding_model反复引用，多线程安全
        // 可以通过以下方法定义多个服务，只要name不同即可，通过名称引用对应的服务
         
        Map properties = new HashMap();

        //定义Xinference数据向量化模型服务，embedding_model为的向量模型服务数据源名称
        properties.put("http.poolNames","embedding_model");

        properties.put("embedding_model.http.hosts","172.24.176.18:9997");//设置向量模型服务地址，这里调用的xinference发布的模型服务

        properties.put("embedding_model.http.timeoutSocket","60000");
        properties.put("embedding_model.http.timeoutConnection","40000");
        properties.put("embedding_model.http.connectionRequestTimeout","70000");
        properties.put("embedding_model.http.maxTotal","100");
        properties.put("embedding_model.http.defaultMaxPerRoute","100");
        //启动Xinference向量模型服务
        HttpRequestProxy.startHttpPools(properties);
        
        String collectionName = "demo";//向量表名称
        //3. 在向量数据源chan_fqa的向量表demo上执行向量检索
        List<List<SearchResp.SearchResult>> searchResults = MilvusHelper.executeRequest("chan_fqa", milvusClientV2 -> {
            Map eparams = new HashMap();
            eparams.put("input","新增了机构");//content向量字段查询条件转换为向量
            eparams.put("model","custom-bge-large-zh-v1.5");//指定Xinference向量模型名称

            //调用的 xinference 发布的向量模型模型服务，将查询条件转换为向量
            XinferenceResponse result = HttpRequestProxy.sendJsonBody("embedding_model",eparams,"/v1/embeddings",XinferenceResponse.class);
            if(result != null){
                List<Data> data = result.getData();
                if(data != null && data.size() > 0 ) {
                    //获取条件转换的向量数据
                    float[] embedding = data.get(0).getEmbedding();

                    //构建检索参数
                    Map searchParams = new LinkedHashMap();
                    searchParams.put("metric_type","COSINE");//采用余弦相似度算法
                    searchParams.put("radius",0.85);//返回content与查询条件相似度为0.85以上的记录
                    String[] array = {"log_id","collecttime","log_content"};//定义要返回的字段清单
                    SearchResp searchR = milvusClientV2.search(SearchReq.builder()
                            .collectionName(collectionName)
                            .data(Collections.singletonList(new FloatVec(embedding)))
                             .annsField("content")//指定向量字段
                            .filter("log_id < 100000")//指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
                             .searchParams(searchParams)
                            .topK(10)
                            .outputFields(Arrays.asList(array))
                            .build());
                    return searchR.getSearchResults();//返回检索结果
                    
                }
            }
            
            return null;
        });
        //打印结果
        System.out.println("\nSearch results:");
        if(searchResults != null) {
            for (List<SearchResp.SearchResult> results : searchResults) {
                for (SearchResp.SearchResult searchResult : results) {
                    System.out.printf("ID: %d, Score: %f, %s\n", (long) searchResult.getId(), searchResult.getScore(), searchResult.getEntity().toString());
                }
            }
        }
    }

    public static void searchIterator(String[] args){

        /**
         * 参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
         */
        //1. 初始化milvus数据源chan_fqa，用来操作向量数据库，一个milvus数据源只需要定义一次即可，后续通过名称chan_fqa反复引用，多线程安全
        // 可以通过以下方法定义多个Milvus数据源，只要name不同即可，通过名称引用对应的数据源
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName("chan_fqa");//数据源名称
        milvusConfig.setDbName("ucr_chan_fqa");//Milvus数据库名称
        milvusConfig.setUri("http://172.24.176.18:19530");//Milvus数据库地址
        milvusConfig.setToken("");//认证token：root:xxxx

        MilvusHelper.init(milvusConfig);//启动初始化Milvus数据源



        //2. 初始化Xinference向量模型服务embedding_model，一个服务组只需要定义一次即可，后续通过名称embedding_model反复引用，多线程安全
        // 可以通过以下方法定义多个服务，只要name不同即可，通过名称引用对应的服务

        Map properties = new HashMap();

        //定义Xinference数据向量化模型服务，embedding_model为的向量模型服务数据源名称
        properties.put("http.poolNames","embedding_model");

        properties.put("embedding_model.http.hosts","172.24.176.18:9997");//设置向量模型服务地址，这里调用的xinference发布的模型服务

        properties.put("embedding_model.http.timeoutSocket","60000");
        properties.put("embedding_model.http.timeoutConnection","40000");
        properties.put("embedding_model.http.connectionRequestTimeout","70000");
        properties.put("embedding_model.http.maxTotal","100");
        properties.put("embedding_model.http.defaultMaxPerRoute","100");
        //启动Xinference向量模型服务
        HttpRequestProxy.startHttpPools(properties);

        String collectionName = "demo";//向量表名称
        //3. 在向量数据源chan_fqa的向量表demo上执行向量检索
        MilvusHelper.executeRequest("chan_fqa", milvusClientV2 -> {
            Map eparams = new HashMap();
            eparams.put("input","新增了机构");//content向量字段查询条件转换为向量
            eparams.put("model","custom-bge-large-zh-v1.5");//指定Xinference向量模型名称

            //调用的 xinference 发布的向量模型模型服务，将查询条件转换为向量
            XinferenceResponse result = HttpRequestProxy.sendJsonBody("embedding_model",eparams,"/v1/embeddings",XinferenceResponse.class);
            if(result != null){
                List<Data> data = result.getData();
                if(data != null && data.size() > 0 ) {
                    //获取条件转换的向量数据
                    float[] embedding = data.get(0).getEmbedding();

             
                    String[] array = {"log_id","collecttime","log_content"};//定义要返回的字段清单
  


                    SearchIterator searchIterator = milvusClientV2.searchIterator(SearchIteratorReq.builder()
                            .collectionName(collectionName)
                            .outputFields(Arrays.asList(array))
                            .batchSize(50L)
                            .vectorFieldName("content")
                            .vectors(Collections.singletonList(new FloatVec(embedding)))
                            .expr("log_id < 100000")
                            .params("{\"radius\": 0.85}") //返回content与查询条件相似度为0.85以上的记录
//                            .topK(300)
                            .metricType(IndexParam.MetricType.COSINE) //采用余弦相似度算法
                            .consistencyLevel(ConsistencyLevel.BOUNDED)
                            .build());

                    while (true) {
                        List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
                        if (res.isEmpty()) {
                            System.out.println("Search iteration finished, close");
                            searchIterator.close();
                            break;
                        }

                        for (QueryResultsWrapper.RowRecord record : res) {
                            System.out.println(record);
                        }
                    }
                    return null;//返回检索结果

                }
            }

            return null;
        });
        
    }

    public static void main(String[] args){
        searchIterator(args);
        queryIterator(args);
//        query(args);
//        search(args);
    }
    public static void query(String[] args){

        /**
         * 参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
         */
        //1. 初始化milvus数据源chan_fqa，用来操作向量数据库，一个milvus数据源只需要定义一次即可，后续通过名称chan_fqa反复引用，多线程安全
        // 可以通过以下方法定义多个Milvus数据源，只要name不同即可，通过名称引用对应的数据源
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName("chan_fqa");//数据源名称
        milvusConfig.setDbName("ucr_chan_fqa");//Milvus数据库名称
        milvusConfig.setUri("http://172.24.176.18:19530");//Milvus数据库地址
        milvusConfig.setToken("");//认证token：root:xxxx

        MilvusHelper.init(milvusConfig);//启动初始化Milvus数据源



        

        String collectionName = "demo";//向量表名称
        //3. 在向量数据源chan_fqa的向量表demo上执行向量检索
        List<QueryResp.QueryResult> searchResults = MilvusHelper.executeRequest("chan_fqa", milvusClientV2 -> {

                   
            String[] array = {"log_id","collecttime","log_content"};//定义要返回的字段清单
            QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                    .collectionName(collectionName)
                    .filter("log_id < 100000")//指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
                    .outputFields(Arrays.asList(array))
//                            .ids(List<Object> ids)
                    .offset(0)
                    .limit(10)
                    .build());
            return queryResp.getQueryResults();//返回检索结果


        });
        //打印结果
        System.out.println("\nSearch results:");
        if(searchResults != null) {
            for (QueryResp.QueryResult result : searchResults) {
                System.out.println(result.getEntity());
            }
        }
    }


    public static void queryIterator(String[] args){

        /**
         * 参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
         */
        //1. 初始化milvus数据源chan_fqa，用来操作向量数据库，一个milvus数据源只需要定义一次即可，后续通过名称chan_fqa反复引用，多线程安全
        // 可以通过以下方法定义多个Milvus数据源，只要name不同即可，通过名称引用对应的数据源
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName("chan_fqa");//数据源名称
        milvusConfig.setDbName("ucr_chan_fqa");//Milvus数据库名称
        milvusConfig.setUri("http://172.24.176.18:19530");//Milvus数据库地址
        milvusConfig.setToken("");//认证token：root:xxxx

        MilvusHelper.init(milvusConfig);//启动初始化Milvus数据源





        String collectionName = "demo";//向量表名称
        //3. 在向量数据源chan_fqa的向量表demo上执行向量检索
         MilvusHelper.executeRequest("chan_fqa", milvusClientV2 -> {


            String[] array = {"log_id","collecttime","log_content"};//定义要返回的字段清单
            QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                    .collectionName(collectionName)
                    .expr("log_id < 100000")//指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
                    .outputFields(Arrays.asList(array))
                    .batchSize(50L)                     
                    .build());

            while (true) {
                List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
                if (res.isEmpty()) {
                    System.out.println("query iteration finished, close");
                    queryIterator.close();
                    break;
                }

                for (QueryResultsWrapper.RowRecord record : res) {
                    System.out.println(record);
                }
            }
            return null;

        });
        
    }

}
