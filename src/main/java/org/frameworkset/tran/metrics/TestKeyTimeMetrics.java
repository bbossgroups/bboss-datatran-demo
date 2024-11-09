package org.frameworkset.tran.metrics;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.*;

import static java.lang.Thread.sleep;

/**
 * <p>Description: 基于key的带交换区指标计算测试用例，在key值无限增长情况下，还能进行正常指标计算</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/16
 * @author biaoping.yin
 * @version 1.0
 */
public class TestKeyTimeMetrics {
	private static Logger logger = LoggerFactory.getLogger(TestKeyTimeMetrics.class);
	public static void main(String[] args){
		//1. 定义Elasticsearch数据入库BulkProcessor批处理组件构建器
		BulkProcessorBuilder bulkProcessorBuilder = new BulkProcessorBuilder();
		bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

				.setBulkSizes(200)//按批处理数据记录数
				.setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

				.setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
				.setWorkThreads(10)//bulk处理工作线程数
				.setWorkThreadQueue(50)//bulk处理工作线程池缓冲队列大小
				.setBulkProcessorName("detail_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
				.setBulkRejectMessage("detail bulkprocessor")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
				.setElasticsearch("default")//指定明细Elasticsearch集群数据源名称，bboss可以支持多数据源
				.setFilterPath(BulkConfig.ERROR_FILTER_PATH)
				.addBulkInterceptor(new BulkInterceptor() {
					public void beforeBulk(BulkCommand bulkCommand) {

					}

					public void afterBulk(BulkCommand bulkCommand, String result) {
						if(logger.isDebugEnabled()){
							logger.debug(result);
						}
					}

					public void exceptionBulk(BulkCommand bulkCommand, Throwable exception) {
						if(logger.isErrorEnabled()){
							logger.error("exceptionBulk",exception);
						}
					}
					public void errorBulk(BulkCommand bulkCommand, String result) {
						if(logger.isWarnEnabled()){
							logger.warn(result);
						}
					}
				})//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
		;
		/**
		 * 2. 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
		 */
		BulkProcessor bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
        //3. 定义KeyTimeMetircs类型指标计算器Metrics
		Metrics keyMetrics = new Metrics(Metrics.MetricsType_KeyTimeMetircs){

            //4. 添加具体的指标对象及对应的指标key到Metrics
			@Override
			public void builderMetrics(){
				addMetricBuilder(new MetricBuilder() {
					@Override
					public MetricKey buildMetricKey(MapData mapData){
						Map data = (Map) mapData.getData();
						String name = (String) data.get("name");
						return new MetricKey(name);
					}
					@Override
					public KeyMetricBuilder metricBuilder(){
						return new KeyMetricBuilder() {
							@Override
							public KeyMetric build() {
								return new TestTimeMetric();
							}
						};
					}
				});
				// 5. key metrics中包含两个segment(S0,S1)，设置每个分区的大小（元素个数）
				setSegmentBoundSize(5000000);
                // 6. 定义时间窗口
				setTimeWindows(60);
			}

            /**
             * 6. 定义指标持久化机制，将计算好的指标结果通过异步批处理组件存储到Elasticsearch中的指标表
             * @param metrics
             */
			@Override
			public void persistent(Collection< KeyMetric> metrics) {
				metrics.forEach(keyMetric->{
					TestTimeMetric testKeyMetric = (TestTimeMetric)keyMetric;
					Map esData = new HashMap();//封装指标统计结果
					esData.put("dataTime", testKeyMetric.getDataTime());//指标统计计算时间
					esData.put("hour", testKeyMetric.getDayHour());//指标小时字段，例如2023-02-19 16
					esData.put("minute", testKeyMetric.getMinute());//指标日期字段，例如2023-02-19 16:53
					esData.put("day", testKeyMetric.getDay());//指标日期字段，例如2023-02-19
					esData.put("metric", testKeyMetric.getMetric());//指标key
					esData.put("name", testKeyMetric.getName());//维度字段
					esData.put("count", testKeyMetric.getCount());//统计指标
					bulkProcessor.insertData("vops-testkeytimemetrics",esData);//将指标结果保存到Elasticsearch指标表vops-testkeytimemetrics

				});

			}
		};
        // 7. 初始化指标计算器
		keyMetrics.init();

        // 8. 模拟并发（10线程）产生数据，并持续运行10分钟
		long startTime = System.currentTimeMillis();
		long times = 10l * 60l * 1000l;

		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				DateFormat yearFormat = MetricsConfig.getYearFormat();
				DateFormat monthFormat = MetricsConfig.getMonthFormat();
				DateFormat weekFormat = MetricsConfig.getWeekFormat();
				DateFormat dayFormat = MetricsConfig.getDayFormat();
				DateFormat hourFormat = MetricsConfig.getHourFormat();
				DateFormat minuteFormat = MetricsConfig.getMinuteFormat();
				while(true) {
					for (int i = 0; i < 1590; i++) {
                        // 9. 通过MapData封装待统计的原始数据（一个Map类型的对象）、时间维度值、各种时间格式化对象
						MapData<Map> mapData = new MapData<>();
						mapData.setDataTime(new Date());
						Map<String, String> data = new LinkedHashMap<>();
						data.put("name", "Jack_" + i);
						mapData.setData(data);
						mapData.setDayFormat(dayFormat);
						mapData.setHourFormat(hourFormat);
						mapData.setMinuteFormat(minuteFormat);
						mapData.setYearFormat(yearFormat);
						mapData.setMonthFormat(monthFormat);
						mapData.setWeekFormat(weekFormat);
                        // 10. 将mapData交给keyMetrics，进行指标统计计算
						keyMetrics.map(mapData);
					}
					try {
						sleep(1000l);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					long now = System.currentTimeMillis();
					long eTime = now - startTime;
					if(eTime >= times)//达到10分钟，终止生产数据
						break;
				}
			}
		};
		List<Thread> ts = new ArrayList<>();
		for(int i = 0; i < 10; i ++){
			Thread thread = new Thread(runnable,"run-"+i);
			thread.start();
			ts.add(thread);
		}
		//等待所有线程运行结束
		ts.forEach(t -> {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

		//强制刷指标数据
		keyMetrics.forceFlush(true,//清楚指标key
                              true );//等待数据处理完成后再返回
	}
}
