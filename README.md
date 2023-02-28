
# 数据同步工具
可以非常方便地实现各种数据源之间的数据采集、同步、数据转换清洗处理、入库功能

本工程提供了非常纯粹的作业开发gradle工程模板，使用本工具所带的应用程序运行容器，可以快速编写，打包发布可运行的数据导入导出、同步作业


支持的数据库：

mysql,maridb，postgress,oracle ,sqlserver,db2、clickhouse等

支持的Elasticsearch版本：

1.x,2.X,5.X,6.X,7.x,8,x,+

支持海量PB级数据同步导入功能

[使用参考文档](https://esdoc.bbossgroups.com/#/db-es-tool)

# BBoss Environmental requirements

JDK requirement: JDK 1.8+

Elasticsearch version requirements: 1.x,2.X,5.X,6.X,7.x,8,x,+

Spring booter 1.x,2.x,+


# 构建部署
## 准备工作
需要通过gradle构建发布版本，gradle安装配置参考文档：

https://esdoc.bbossgroups.com/#/bboss-build

## 下载源码工程-基于gradle
https://gitee.com/bboss/bboss-datatran-demo

从上面的地址下载源码工程，然后导入idea或者eclipse，根据自己的需求，修改导入程序逻辑

org.frameworkset.elasticsearch.imp.Db2EleasticsearchDemo

如果需要测试和调试导入功能，运行Db2EleasticsearchDemo的main方法即可即可：


```java
public class Db2EleasticsearchDemo {
	public static void main(String args[]){

		Db2EleasticsearchDemo db2EleasticsearchDemo = new Db2EleasticsearchDemo();
        		//从配置文件application.properties中获取参数值
        		boolean dropIndice = PropertiesUtil.getPropertiesContainer().getBooleanSystemEnvProperty("dropIndice",true);
        
        		db2EleasticsearchDemo.scheduleTimestampImportData(dropIndice);
	}
    .....
}
```

测试调试通过后
修改application.properties中的mainclass配置。
mainclass=org.frameworkset.elasticsearch.imp.Db2EleasticsearchDemo

构建发布可运行的版本：进入命令行模式，在源码工程根目录bboss-datatran-demo下运行以下gradle指令打包发布版本
release.bat

## 运行作业
gradle构建成功后，在build/distributions目录下会生成可以运行的zip包，解压运行导入程序

linux：

chmod +x restart.sh

./restart.sh

windows: restart.bat

## 作业jvm配置
修改jvm.options，设置内存大小和其他jvm参数

-Xms1g

-Xmx1g

## 在工程中添加多个表同步作业
默认的作业任务是Dbdemo，同步表td_sm_log的数据到索引dbdemo/dbdemo中

现在我们在工程中添加另外一张表td_cms_document的同步到索引cms_document/cms_document的作业步骤：

1.首先，新建一个带main方法的类org.frameworkset.elasticsearch.imp.CMSDocumentImport,实现同步的逻辑

如果需要测试调试，编写 src\main\java\org\frameworkset\elasticsearch\imp\CMSDocumentImportTest.java测试类,然后debug即可

2.然后，在runfiles目录下新建CMSDocumentImport作业主程序和作业进程配置文件：runfiles/config-cmsdocmenttable.properties，内容如下：

mainclass=org.frameworkset.elasticsearch.imp.CMSDocumentImport

pidfile=CMSDocumentImport.pid  


3.最后在runfiles目录下新建作业启动sh文件（这里只新建linux/unix指令，windows的类似）：runfiles/restart-cmsdocumenttable.sh

内容与默认的作业任务是Dbdemo内容一样，只是在java命令后面多加了一个参数，用来指定作业配置文件：--conf=config-cmsdocmenttable.properties

nohup java \$RT_JAVA_OPTS -jar ${project}-${bboss_version}.jar restart --conf=config-cmsdocmenttable.properties --shutdownLevel=9 > ${project}.log &

其他stop shell指令也类似建立即可

# 管理提取数据的sql语句

db2es工具管理提取数据的sql语句有两种方法：代码中直接编写sql语句，配置文件中采用sql语句  
## 1.代码中写sql

		`//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
		// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
		// select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
		// log_id和数据库对应的字段一致,就不需要设置setLastValueColumn信息，
		// 但是需要设置setLastValueType告诉工具增量字段的类型
	
		importBuilder.setSql("select * from td_sm_log where log_id > #[log_id]");`
## 2.在配置文件中管理sql
设置sql语句配置文件路径和对应在配置文件中sql name
`importBuilder.setSqlFilepath("sql.xml")
​			 .setSqlName("demoexportFull");	`	

配置文件sql.xml，编译到classes根目录即可：

<?xml version="1.0" encoding='UTF-8'?>
<properties>
​    <description>
​        <![CDATA[
​	配置数据导入的sql
 ]]>
​    </description>
​    <property name="demoexport"><![CDATA[select * from td_sm_log where log_id > #[log_id]]]></property>
​    <property name="demoexportFull"><![CDATA[select * from td_sm_log ]]></property>

</properties>

在sql配置文件中可以配置多条sql语句

# 作业参数配置

在使用[bboss-datatran-demo](https://github.com/bbossgroups/bboss-datatran-demo)时，为了避免调试过程中不断打包发布数据同步工具，可以将部分控制参数配置到启动配置文件resources/application.properties中,然后在代码中通过以下方法获取配置的参数：

```ini
#工具主程序
mainclass=org.frameworkset.elasticsearch.imp.Dbdemo

# 参数配置
# 在代码中获取方法：PropertiesUtil.getPropertiesContainer().getBooleanAttribute("dropIndice",false);//同时指定了默认值false
dropIndice=false
```

在代码中获取参数dropIndice方法：

```java
boolean dropIndice = PropertiesUtil.getPropertiesContainer().getBooleanAttribute("dropIndice",false);//同时指定了默认值false
```

另外可以在resources/application.properties配置控制作业执行的一些参数，例如工作线程数，等待队列数，批处理size等等：

```
queueSize=50
workThreads=10
batchSize=20
```

在作业执行方法中获取并使用上述参数：

```java
int batchSize = PropertiesUtil.getPropertiesContainer().getIntSystemEnvProperty("batchSize",10);//同时指定了默认值
int queueSize = PropertiesUtil.getPropertiesContainer().getIntSystemEnvProperty("queueSize",50);//同时指定了默认值
int workThreads = PropertiesUtil.getPropertiesContainer().getIntSystemEnvProperty("workThreads",10);//同时指定了默认值
importBuilder.setBatchSize(batchSize);
importBuilder.setQueue(queueSize);//设置批量导入线程池等待队列长度
importBuilder.setThreadCount(workThreads);//设置批量导入线程池工作线程数量
```


# 建表sql
```
mysql :
CREATE TABLE
    batchtest
    (
        id bigint NOT NULL AUTO_INCREMENT,
        name VARCHAR(4000),
        author VARCHAR(1000),
        content longtext,
        title VARCHAR(1000),
        optime DATETIME,
        oper VARCHAR(1000),
        subtitle VARCHAR(1000),
        collecttime DATETIME,
        ipinfo VARCHAR(2000),
        PRIMARY KEY (id)
    )
    ENGINE=InnoDB DEFAULT CHARSET=utf8;
postgresql:

CREATE TABLE
    batchtest
    (
        id bigint ,
        name VARCHAR(4000),
        author VARCHAR(1000),
        content text,
        title VARCHAR(1000),
        optime timestamp,
        oper VARCHAR(1000),
        subtitle VARCHAR(1000),
        collecttime timestamp,
        ipinfo VARCHAR(2000),
        PRIMARY KEY (id)
    )
```     

## elasticsearch技术交流群:166471282 

## elasticsearch微信公众号:bbossgroup   
![GitHub Logo](https://static.oschina.net/uploads/space/2017/0617/094201_QhWs_94045.jpg)


