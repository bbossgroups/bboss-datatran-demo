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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.excel.ExcelFileConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.builder.*;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.listener.JobFlowListener;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.file.input.ExcelFileInputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.TimeUtil;
import org.frameworkset.util.concurrent.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: 作业工作流demo</p>
 *
 * @author biaoping.yin
 * @Date 2025/6/11
 */
public class JobFlowTest {
    private static Logger logger = LoggerFactory.getLogger(JobFlowTest.class);
    private static ImportBuilder buildFile2DB(){
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setBatchSize(500)//设置批量入库的记录数
                .setFetchSize(1000);//设置按批读取文件行数

        importBuilder.addCallInterceptor(new CallInterceptor() {
            @Override
            public void preCall(TaskContext taskContext) {
                //任务调用前，向流程执行上下文中添加参数
                taskContext.addJobFlowContextData("flowParam","测试");
                //任务调用前，向往流程节点执行上下文中添加参数
                taskContext.addJobFlowNodeBuilderContextData("nodeParam","测试");
                //任务调用前，向流程子节点所属的复合节点（串行/并行）执行上下文中添加参数
                if(taskContext.getContainerJobFlowNodeExecuteContext() != null)
                    taskContext.addContainerJobFlowNodeContextData("nodeParam","测试");
            }

            @Override
            public void afterCall(TaskContext taskContext) {
                  //作业执行完后，添加记录处理统计数据到流程执行上下文中（流程节点，流程子节点所属的复合节点（串行/并行）类似）
                String nodeId = taskContext.getJobFlowNodeExecuteContext().getNodeId();
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalSuccessRecords",taskContext.getJobTaskMetrics().getTotalSuccessRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalFailedRecords",taskContext.getJobTaskMetrics().getTotalFailedRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalRecords",taskContext.getJobTaskMetrics().getTotalRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalIgnoreRecords",taskContext.getJobTaskMetrics().getTotalIgnoreRecords());
                

            }

            @Override
            public void throwException(TaskContext taskContext, Throwable e) {
                //作业执行异常后，添加记录处理统计数据到流程执行上下文中（流程节点，流程子节点所属的复合节点（串行/并行）类似）
                String nodeId = taskContext.getJobFlowNodeExecuteContext().getNodeId();
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalSuccessRecords",taskContext.getJobTaskMetrics().getTotalSuccessRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalFailedRecords",taskContext.getJobTaskMetrics().getTotalFailedRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalRecords",taskContext.getJobTaskMetrics().getTotalRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".totalIgnoreRecords",taskContext.getJobTaskMetrics().getTotalIgnoreRecords());
                taskContext.addJobFlowContextData("flowParam."+nodeId+".errorinfo",SimpleStringUtil.exceptionToString(e));

            }
        });

        ExcelFileInputConfig config = new ExcelFileInputConfig();
        config.setDisableScanNewFiles(true);
        config.setDisableScanNewFilesCheckpoint(false);
        //shebao_org,person_no, name, cert_type,cert_no,zhs_item  ,zhs_class ,zhs_sub_class,zhs_year  , zhs_level
        //配置excel文件列与导出字段名称映射关系
        FileConfig excelFileConfig = new ExcelFileConfig();
        excelFileConfig
                .addCellMapping(0,"shebao_org")
                .addCellMapping(1,"person_no")
                .addCellMapping(2,"name")
                .addCellMapping(3,"cert_type")

                .addCellMapping(4,"cert_no","")
                .addCellMapping(5,"zhs_item")

                .addCellMapping(6,"zhs_class")
                .addCellMapping(7,"zhs_sub_class")
                .addCellMapping(8,"zhs_year","2022")
                .addCellMapping(9,"zhs_level","1");
        excelFileConfig.setSourcePath("C:\\workspace\\bbossgroups\\bboss-demos\\etl-elasticsearch\\filelog-elasticsearch\\excelfiles")//指定目录
                .setFileFilter(new FileFilter() {
                    @Override
                    public boolean accept(FilterFileInfo fileInfo, FileConfig fileConfig) {
                        //判断是否采集文件数据，返回true标识采集，false 不采集
                        return fileInfo.getFileName().equals("cityperson.xlsx");
                    }
                })//指定文件过滤器
                .setDeleteEOFFile(false)
                .setSkipHeaderLines(1);//忽略第一行
        config.addConfig(excelFileConfig);
        config.setEnableMeta(true);
        importBuilder.setInputConfig(config);
        //指定elasticsearch数据源名称，在application.properties文件中配置，default为默认的es数据源名称

//导出到数据源配置
        DBOutputConfig dbOutputConfig = new DBOutputConfig();
        dbOutputConfig
                .setSqlFilepath("sql-dbtran.xml")
                .setDbName("test")//指定目标数据库，在application.properties文件中配置
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true") 
                .setDbUser("root")
                .setDbPassword("123456")
                .setValidateSQL("select 1")
                .setUsePool(true)//是否使用连接池
                .setDbMaxSize(100)
                .setDbMinIdleSize(100)
                .setDbInitSize(100)
                .setInsertSqlName("insertcityperson");//指定新增的sql语句名称，在配置文件中配置：sql-dbtran.xml

        importBuilder.setOutputConfig(dbOutputConfig);

        final Count count = new Count();
        /**
         * 重新设置es数据结构
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception  {

                //shebao_org,person_no, name, cert_type,cert_no,zhs_item  ,zhs_class ,zhs_sub_class,zhs_year  , zhs_level
                //从流程执行上下文档中获取参数（流程节点，流程子节点所属的复合节点（串行/并行）类似）
                Object flowParam = context.getJobFlowExecuteContext().getContextData("flowParam");
//                Object flowParam = context.getJobFlowExecuteContext().getContextData("flowParam","defaultValue");  指定默认值
                context.addFieldValue("rowNo",count.getCount());
                count.increament();

//				logger.info(SimpleStringUtil.object2json(values));
            }
        });


        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setThreadCount(5);
        importBuilder.setLastValueStorePath("C:\\workdir\\exception\\jobflowstatus");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点
        return importBuilder;
    }

    private static ImportBuilder buildDB2Custom(int id){
        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder.setBatchSize(500)//设置批量入库的记录数
                .setFetchSize(1000);//设置按批读取文件行数

        DBInputConfig dbInputConfig = new DBInputConfig();
        dbInputConfig.setDbName("source"+id)
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true")
                .setDbUser("root")
                .setDbPassword("123456")
                .setValidateSQL("select 1")
                .setUsePool(false)//是否使用连接池
                .setSqlFilepath("sql.xml")
                .setSqlName("demoexportLimit").setParallelDatarefactor(true);
        importBuilder.setInputConfig(dbInputConfig); 
        //指定elasticsearch数据源名称，在application.properties文件中配置，default为默认的es数据源名称

//导出到数据源配置
        CustomOutputConfig customOutputConfig = new CustomOutputConfig();
        customOutputConfig.setCustomOutPut(new CustomOutPut() {
            @Override
            public void handleData(TaskContext taskContext, List<CommonRecord> datas) {

                
                String flowParam = (String) taskContext.getJobFlowExecuteContext().getContextData("flowParam");
                //You can do any thing here for datas
                for(CommonRecord record:datas){
                    Map<String,Object> data = record.getDatas();
                    logger.info("data:{}",SimpleStringUtil.object2json(data));
                    logger.info("Meta:{}",SimpleStringUtil.object2json(record.getMetaDatas()));

                }
            }
        });
        importBuilder.setOutputConfig(customOutputConfig);

        


        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setThreadCount(5);
       
        return importBuilder;
    }
    public static void main(String[] args){
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        JobFlowScheduleConfig jobFlowScheduleConfig = new JobFlowScheduleConfig();
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateHours(new Date(),2));//2小时后开始执行
//        jobFlowScheduleConfig.setScheduleDate(TimeUtil.addDateMinitues(new Date(),1));//1分钟后开始执行
//        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDates(new Date(),10));//10天后结束
        jobFlowScheduleConfig.setScheduleEndDate(TimeUtil.addDateMinitues(new Date(),10));//2分钟后结束
        jobFlowScheduleConfig.setPeriod(1000000L);
        jobFlowScheduleConfig.setExecuteOneTime(true);
//        jobFlowScheduleConfig.addScanNewFileTimeRange("12:00-13:00");
//        jobFlowScheduleConfig.addSkipScanNewFileTimeRange("12:00-13:00");
        jobFlowBuilder.setJobFlowScheduleConfig(jobFlowScheduleConfig);
        
        //为流程添加监听器
        jobFlowBuilder.addJobFlowListener(new JobFlowListener() {
            @Override
            public void beforeStart(JobFlow jobFlow) {

            }

            @Override
            public void beforeExecute(JobFlowExecuteContext jobFlowExecuteContext) {

            }

            @Override
            public void afterExecute(JobFlowExecuteContext jobFlowExecuteContext, Throwable throwable) {
                //打印流程执行监控指标
                logger.info(SimpleStringUtil.object2json(jobFlowExecuteContext.getJobFlowStaticContext()));

            }

            @Override
            public void afterEnd(JobFlow jobFlow) {

            }
        });
        /**
         * 1.构建第一个任务节点：单任务节点
         */
        DatatranJobFlowNodeBuilder jobFlowNodeBuilder = new DatatranJobFlowNodeBuilder("1","DatatranJobFlowNode");
        NodeTrigger nodeTrigger = new NodeTrigger();

        String script = new StringBuilder()
                .append("[import]")
                .append("//导入脚本中需要引用的java类\r\n")
                .append(" //import org.frameworkset.tran.jobflow.context.StaticContext; ")
                .append("[/import]")
                .append("StaticContext staticContext = nodeTriggerContext.getPreJobFlowStaticContext();")
                .append("//前序节点执行异常结束，则忽略当前节点执行\r\n")
                .append("if(staticContext != null && staticContext.getExecuteException() != null)")
                .append("    return false;")
                .append("else{")
                .append("    return true;")
                .append("}").toString();
        nodeTrigger.setTriggerScript(script);

        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setImportBuilder(buildFile2DB()).setNodeTrigger(nodeTrigger);
        /**
         * 1.2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
        
        /**
         * 2.构建第二个任务节点：并行任务节点
         */
        ParrelJobFlowNodeBuilder parrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder("2","ParrelJobFlowNode");
        //为并行任务节点添加触发器
        NodeTrigger parrelnewNodeTrigger = new NodeTrigger();
        parrelnewNodeTrigger.setTriggerScriptAPI(new TriggerScriptAPI() {
            @Override
            public boolean needTrigger(NodeTriggerContext nodeTriggerContext) throws Exception {
                
                return true;
            }
        });
        parrelJobFlowNodeBuilder.setNodeTrigger(parrelnewNodeTrigger);

        //为并行任务节点添加监听器
        
        parrelJobFlowNodeBuilder.addJobFlowNodeListener(new JobFlowNodeListener() {
            @Override
            public void beforeExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {

            }

            @Override
            public void afterExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, Throwable throwable) {
                //打印流程节点执行监控指标
                logger.info(SimpleStringUtil.object2json(jobFlowNodeExecuteContext.getJobFlowNodeStaticContext()));
            }

            @Override
            public void afterEnd(JobFlowNode jobFlowNode) {

            }
        });
        /**
         * 2.1 为第二个并行任务节点添加第一个带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new DatatranJobFlowNodeBuilder("ParrelJobFlowNode-DatatranJobFlowNode-2-1","ParrelJobFlowNode-DatatranJobFlowNode-2")
                .setImportBuilder(buildDB2Custom(1))
                        .setNodeTrigger(nodeTrigger));
        /**
         * 2.2 为第二个并行任务节点添加第二个不带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new DatatranJobFlowNodeBuilder("ParrelJobFlowNode-DatatranJobFlowNode-2-2","ParrelJobFlowNode-DatatranJobFlowNode-2")
                                                        .setImportBuilder(buildDB2Custom(2)));
        /**
         * 2.3 为第二个并行任务节点添加第三个串行复杂流程子任务
         */
        SequenceJobFlowNodeBuilder comJobFlowNodeBuilder = new SequenceJobFlowNodeBuilder("ParrelJobFlowNode-2-3","SequenceJobFlowNode");
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new DatatranJobFlowNodeBuilder("ParrelJobFlowNode-2-3-1","SequenceJobFlowNode-SequenceJobFlowNode")
                .setImportBuilder(buildDB2Custom(3)));
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new DatatranJobFlowNodeBuilder("ParrelJobFlowNode-2-3-2","SequenceJobFlowNode-SequenceJobFlowNode")
                        .setImportBuilder(buildDB2Custom(4)));
        comJobFlowNodeBuilder.addJobFlowNodeBuilder(new CommonJobFlowNodeBuilder("ParrelJobFlowNode-2-3-3","SequenceJobFlowNode-SequenceJobFlowNode",new JobFlowNodeFunctionTest(false)).setNodeTrigger(nodeTrigger) );

        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(comJobFlowNodeBuilder);

        ParrelJobFlowNodeBuilder subParrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder("ParrelJobFlowNode-2-4","ParrelJobFlowNode");
        subParrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new DatatranJobFlowNodeBuilder("ParrelJobFlowNode-2-4-1","ParrelJobFlowNode-SequenceJobFlowNode")
                .setImportBuilder(buildDB2Custom(5)));
        subParrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new DatatranJobFlowNodeBuilder("ParrelJobFlowNode-2-4-2","ParrelJobFlowNode-SequenceJobFlowNode")
                .setImportBuilder(buildDB2Custom(6)));
        /**
         * 2.4 为第二个并行任务节点添加第三个并行行复杂流程子任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(subParrelJobFlowNodeBuilder);

        /**
         * 2.5 将第二个节点添加到工作流中
         */
        jobFlowBuilder.addJobFlowNodeBuilder(parrelJobFlowNodeBuilder);

        /**
         * 3.构建第三个任务节点：单任务节点
         */
        jobFlowNodeBuilder = new DatatranJobFlowNodeBuilder("3","DatatranJobFlowNode");
        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.setImportBuilder(buildDB2Custom(7)).setNodeTrigger(nodeTrigger);
            
        /**
         * 1.2 将第三个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
        
        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
//        
        jobFlow.stop();
//
        jobFlow.pause();
//        
        jobFlow.consume();
        

    }
}
