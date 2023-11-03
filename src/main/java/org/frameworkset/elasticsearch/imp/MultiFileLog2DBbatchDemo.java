package org.frameworkset.elasticsearch.imp;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.db.output.SQLConf;
import org.frameworkset.tran.plugin.db.output.SQLConfResolver;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: 从日志文件采集日志数据并保存到</p>
 * <p>多文件分批批量导入多表</p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class MultiFileLog2DBbatchDemo {
    private static Logger logger = LoggerFactory.getLogger(MultiFileLog2DBbatchDemo.class);

    /**
     * 创建目录
     *
     * @param path
     */
    public static void CreatFileDir(String path) {
        try {
            File file = new File(path);
            if (file.getParentFile().isDirectory()) {//判断上级目录是否是目录
                if (!file.exists()) {   //如果文件目录不存在
                    file.mkdirs();  //创建文件目录
                }
            } else {
                throw new Exception("传入目录非标准目录名");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //LocalPoolDeployer.addShutdownHook = true;//在应用程序stop.sh时，关闭数据源错误提示
        //启动数据采集
        MultiFileLog2DBbatchDemo file2DB = new MultiFileLog2DBbatchDemo();
        file2DB.scheduleTimestampImportData();
    }


    public static void scheduleTimestampImportData() {

        ImportBuilder importBuilder = new ImportBuilder();
        FileInputConfig config = new FileInputConfig();

        config.setJsondata(true);//标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
        config.setRootLevel(true);//jsondata = true时，自定义的数据是否和采集的数据平级，true则直接在原先的json串中存放数据 false则定义一个json存放数据，若不是json则是message

        String data_dir = CommonLauncher.getProperty("DATA_DIR", "D:\\www\\input_data\\");  //数据获取目录
        long backup_date = CommonLauncher.getLongAttribute("BACKUP_DATE", 10000l);  //数据备份周期

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date _startDate = null;
        try {
            _startDate = format.parse("20201211");//下载和采集2020年12月11日以后的数据文件
        } catch (ParseException e) {
            logger.error("", e);
        }
        final Date startDate = _startDate;
        config.setBackupSuccessFiles(true);//备份采集完成的日志文件
        config.setBackupSuccessFileLiveTime(10 * 24 * 60 * 60 * 1000l);//备份文件保留时长，单位：毫秒
        config.setBackupSuccessFileDir("d:/backupdir");//文件备份目录
        config.addConfig(new FileConfig()
                        .setSourcePath(data_dir)//指定目录
                        .setFileHeadLineRegular("")//指定多行记录的开头识别标记，正则表达式
                        .setFileFilter(new FileFilter() {
                            @Override
                            public boolean accept(FilterFileInfo fileInfo, FileConfig fileConfig) {

                                if (fileInfo.isDirectory())//由于要采集子目录下的文件，所以如果是目录则直接返回true，当然也可以根据目录名称决定哪些子目录要采集
                                    return true;
                                String fileName = fileInfo.getFileName();//获取文件名称
                                //判断是否采集文件数据，返回true标识采集，false 不采集

                                //动态加载配置文件
                                PropertiesContainer propertiesContainerPreCall = new PropertiesContainer();
                                propertiesContainerPreCall.addConfigPropertiesFile("dsl-preFileNames.properties");//加载配置文件
                                String fileNameRegular = propertiesContainerPreCall.getSystemEnvProperty("fileNameRegular", "tz_lbsgps_,ZLS_WATER_BILL_EXT_");

                                //2、文件名fileName与可以允许的采集文件名规则进行比对,动态数据插入
                                String[] preFilesNameArr = fileNameRegular.split(",");//文件前缀分隔成数组
                                for (int i = 0; i < preFilesNameArr.length; i++) {
                                    boolean nameMatch = fileName.startsWith(preFilesNameArr[i]);//文件前缀
                                    if (nameMatch) {
                                        //System.out.println("测试name:" + name);
                                        //指定新增的sql语句名称，在配置文件中配置：sql-dbtran.xml
                                        //System.out.println("测试name:" + name);
                                        String day = fileName.substring(preFilesNameArr[i].length());
                                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                                        try {
                                            Date fileDate = format.parse(day);
                                            if (fileDate.after(startDate))//下载和采集2020年12月11日以后的数据文件
                                                return true;
                                        } catch (ParseException e) {
                                            logger.error("", e);
                                        }
                                        break;
                                    }
                                }

                                return false;
                            }
                        })//指定文件过滤器
                        .setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
                        //.addField("tag","elasticsearch")//添加字段tag到记录中
                        .setEnableInode(false)
                //				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
                //.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
        );


//        config.addConfig(new FileConfig(data_dir,//指定目录
//                        fileNameRegular,//指定文件名称，可以是正则表达式
//                        "")//指定多行记录的开头识别标记，正则表达式
//                        .setCloseEOF(true)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
////				.setMaxBytes(1048576)//控制每条日志的最大长度，超过长度将被截取掉
//                        //.setStartPointer(1000l)//设置采集的起始位置，日志内容偏移量
//                        //.addField("tag","error") //添加字段tag到记录中
//                        //.setExcludeLines(new String[]{"\\[DEBUG\\]"})
//                        .setEnableInode(false)
//        );//不采集debug日志

        //config.addConfig(new FileConfig().setSourcePath("D:\\www\\input_data\\logs\\")//指定目录
        //                //.setFileHeadLineRegular("^\\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
        //                .setFileFilter(new FileFilter() {
        //                    @Override
        //                    public boolean accept(File dir, String name, FileConfig fileConfig) {
        //                        //判断是否采集文件数据，返回true标识采集，false 不采集
        //                        return name.equals("metrics-report.log");
        //                    }
        //                })//指定文件过滤器
        //                .setCloseEOF(true)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
        //                //.addField("tag", "elasticsearch")//添加字段tag到记录中
        //                .setEnableInode(false)
        //        //				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
        //        //.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
        //);

//		config.addConfig("E:\\ELK\\data\\data3",".*.txt","^[0-9]{4}-[0-9]{2}-[0-9]{2}");
        /**
         * 启用元数据信息到记录中，元数据信息以map结构方式作为@filemeta字段值添加到记录中，文件插件支持的元信息字段如下：
         * hostIp：主机ip
         * hostName：主机名称
         * filePath： 文件路径
         * timestamp：采集的时间戳
         * pointer：记录对应的截止文件指针,long类型
         * fileId：linux文件号，windows系统对应文件路径
         * 例如：
         * {
         *   "_index": "filelog",
         *   "_type": "_doc",
         *   "_id": "HKErgXgBivowv_nD0Jhn",
         *   "_version": 1,
         *   "_score": null,
         *   "_source": {
         *     "title": "解放",
         *     "subtitle": "小康",
         *     "ipinfo": "",
         *     "newcollecttime": "2021-03-30T03:27:04.546Z",
         *     "author": "张无忌",
         *     "@filemeta": {
         *       "path": "D:\\ecslog\\error-2021-03-27-1.log",
         *       "hostname": "",
         *       "pointer": 3342583,
         *       "hostip": "",
         *       "timestamp": 1617074824542,
         *       "fileId": "D:/ecslog/error-2021-03-27-1.log"
         *     },
         *     "@message": "[18:04:40:161] [INFO] - org.frameworkset.tran.schedule.ScheduleService.externalTimeSchedule(ScheduleService.java:192) - Execute schedule job Take 3 ms"
         *   }
         * }
         *
         * true 开启 false 关闭
         */
        config.setEnableMeta(true);
        importBuilder.setInputConfig(config);
        //指定elasticsearch数据源名称，在application.properties文件中配置，default为默认的es数据源名称

        //导出到数据源配置
        DBOutputConfig dbOutputConfig = new DBOutputConfig();
        dbOutputConfig
                .setSqlFilepath("dsl2ndSqlFile.xml")
                .setDbName("test")
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
                .setDbUser("root")
                .setDbPassword("123456")
                .setValidateSQL("select 1")
                .setUsePool(true)
                .setDbInitSize(5)
                .setDbMinIdleSize(5)
                .setDbMaxSize(10)
                .setShowSql(true);//是否使用连接池
        //设置不同表对应的增删改sql语句
        SQLConf sqlConf = new SQLConf();
        sqlConf.setInsertSqlName("insertcitypersonSQL");//对应sql配置文件dsl2ndSqlFile.xml配置的sql语句insertcitypersonSQL
//        sqlConf.setUpdateSqlName("insertcitypersonUpdateSQL");//可选
//        sqlConf.setDeleteSqlName("insertcitypersonDeleteSQL");//可选
        dbOutputConfig.addSQLConf("cityperson.txt",sqlConf);//用文件名映射该文件对应的sql语句

        sqlConf = new SQLConf();
        sqlConf.setInsertSqlName("insertbatchtestSQL");//对应sql配置文件dsl2ndSqlFile.xml配置的sql语句insertbatchtestSQL
//        sqlConf.setUpdateSqlName("insertbatchtestUpdateSQL");//可选
//        sqlConf.setDeleteSqlName("insertbatchtestDeleteSQL");//可选
        dbOutputConfig.addSQLConf("batchtest.txt",sqlConf);//用文件名映射该文件对应的sql语句

        dbOutputConfig.setSqlConfResolver(new SQLConfResolver() {
            @Override
            public String resolver(TaskContext taskContext, CommonRecord record) {
                String filePath = (String)record.getData("filePath");
                if(filePath.endsWith("batchtest.txt"))
                    return "batchtest.txt";
                else if(filePath.endsWith("cityperson.txt"))
                    return "cityperson.txt";
                return "cityperson.txt";
            }
        });
        importBuilder.setOutputConfig(dbOutputConfig);



        //增量配置开始
        importBuilder.setFromFirst(false);
        //setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
        //setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
        importBuilder.setLastValueStorePath("filelog2db");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        //增量配置结束




        //映射和转换配置开始
        importBuilder.setExportResultHandler(new ExportResultHandler<String, String>() {
            @Override
            public void success(TaskCommand<String, String> taskCommand, String result) {
                TaskMetrics taskMetric = taskCommand.getTaskMetrics();
                logger.info("处理耗时：" + taskCommand.getElapsed() + "毫秒");
                logger.info(taskMetric.toString());
                //logger.info("result:" + result);
            }

            @Override
            public void error(TaskCommand<String, String> taskCommand, String result) {
                logger.warn("error:" + result);
            }

            @Override
            public void exception(TaskCommand<String, String> taskCommand, Throwable exception) {
                logger.warn("处理异常error:", exception);

            }


        });
        //映射和转换配置结束

        /**
         * 重新设置es数据结构
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {
                String filePath = (String)context.getMetaValue("filePath");
                context.addFieldValue("filePath",filePath);
                //context.markRecoredInsert();//添加，默认值,如果不显示标注记录状态则默认为添加操作，对应Elasticsearch的index操作
                //
                //context.markRecoredUpdate();//修改，对应Elasticsearch的update操作
                //
                //context.markRecoredDelete();//删除，对应Elasticsearch的delete操作


                //可以根据条件定义是否丢弃当前记录
                //context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
//				System.out.println(data);

//				  context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
//                context.addFieldValue("author", "duoduo");
//                context.addFieldValue("title", "解放");
//                context.addFieldValue("subtitle", "小康");
//
//                context.addFieldValue("collecttime", new Date());


//				//如果日志是普通的文本日志，非json格式，则可以自己根据规则对包含日志记录内容的message字段进行解析
//				String message = (String) context.getRecord();
//                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//                System.out.println(context.getRecord());
//                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//				String[] fvs = message.split(" ");//空格解析字段


                //对日期字段类型,进行数据转换
                //String date_formate = "yyyy-MM-dd HH:mm:ss";
                //SimpleDateFormat dateFormat = new SimpleDateFormat(date_formate);
                //Object value = context.getValue("CERT_EXPIRY_DATE");
                ////判别对象值是否为空
                //if (value == null || "".equals(value)) {
                //    value = null;
                //} else {
                //    System.out.println("日期类型" +   value);
                //    //进行处理
                //    Object newValue = dateFormat.format(value);
                //    java.sql.Date  sqlDate  =  new java.sql.Date(newValue);
                //    context.addFieldValue("CERT_EXPIRY_DATE", newValue);//替换原有字段的值
                //}
                /**
                 * //解析示意代码
                 * String[] fvs = message.split(" ");//空格解析字段
                 * //将解析后的信息添加到记录中
                 * context.addFieldValue("f1",fvs[0]);
                 * context.addFieldValue("f2",fvs[1]);
                 * context.addFieldValue("logVisitorial",fvs[2]);//包含ip信息
                 */
                //直接获取文件元信息
//				Map fileMata = (Map)context.getValue("@filemeta");
                /**
                 * 文件插件支持的元信息字段如下：
                 * hostIp：主机ip
                 * hostName：主机名称
                 * filePath： 文件路径
                 * timestamp：采集的时间戳
                 * pointer：记录对应的截止文件指针,long类型
                 * fileId：linux文件号，windows系统对应文件路径
                 */
                //String FLOW_ID = (String) context.getValue("FLOW_ID");
                //System.out.println(FLOW_ID);

                //String hostIp = (String) context.getMetaValue("hostIp");
                //String hostName = (String) context.getMetaValue("hostName");
                //String fileId = (String) context.getMetaValue("fileId");
                //long pointer = (long) context.getMetaValue("pointer");
                //context.addFieldValue("filePath", filePath);
                //context.addFieldValue("hostIp", hostIp);
                //context.addFieldValue("hostName", hostName);
                //context.addFieldValue("fileId", fileId);
                //context.addFieldValue("pointer", pointer);
                //context.addIgnoreFieldMapping("@filemeta");

            }
        });
        //映射和转换配置结束

        //配置文件application.properties中获取
        int batchSize = CommonLauncher.getIntProperty("batchSize", 1000);//设置按批读取文件行数
        int queueSize = CommonLauncher.getIntProperty("queueSize", 40);//设置批量导入线程池等待队列长度
        int workThreads = CommonLauncher.getIntProperty("workThreads", 50);//设置批量导入线程池工作线程数量
        int fetchSize = CommonLauncher.getIntProperty("fetchSize", 1000);//同时指定了批读取文件行数的默认值
        int tranDataBufferQueue = CommonLauncher.getIntProperty("tranDataBufferQueue", 10);//控制发送队列长度
        boolean dataAsyn = CommonLauncher.getBooleanAttribute("dataAsyn", false);

        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        //importBuilder.setUseLowcase(false);
        //importBuilder.setUseJavaName(false);

        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        // * 源数据批量预加载队列大小，需要用到的最大缓冲内存为：
        //*  tranDataBufferQueue * fetchSize * 单条记录mem大小
        importBuilder.setFetchSize(fetchSize);//设置按批读取文件行数
        importBuilder.setTranDataBufferQueue(tranDataBufferQueue);//控制发送队列长度，可以控制采集速度,默认长度是10
        //设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
        importBuilder.setFlushInterval(10000l);

        importBuilder.setBatchSize(batchSize);//设置批量入库的记录数
        importBuilder.setQueue(queueSize);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(workThreads);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
        importBuilder.setPrintTaskLog(false);
        /**
         * 启动es数据导入文件并上传sftp/ftp作业
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//启动同步作业
        logger.info("job started.");
    }
}
