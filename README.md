# dbSub

#### 介绍
基于akkaCluster的分布式 binlog 订阅服务,客户端自动向在线binlog服务注册自身,集群服务按照数据库实例区分,客户端订阅多个数据库实例
的请求可能会均衡到多个binlog服务上,集群保证单台binlog服务端宕机不影响客户端继续订阅事件,系统会自动切换客户端连接的binlog服务端以持续
提供事件同步.


#### 软件架构
1: AkkaCluster<br/>
2: SpringBoot<br/>
3: LevelDB 持久事件<br/>


#### 安装教程

mvn clean install -DskipTests=true

java -jar dbSub-0.0.1-SNAPSHOT.jar -bc=initBinlog.xml -cc=binlogAkkaServer.conf

Usage:  [options]
 

  * -binlogConfig, -bc
      启动的binlog配置文件(.xml)
  * -clusterConfig, -cc
      启动的akka集群的配置文件(.conf)
  *  -workFolder, -wf
      服务存储文件的目录(默认为当前目录--[.]代表当前目录)
      Default: .
  *  -maxStore, -ms
      存储的单表的记录数上限,超过该数会清理掉历史数据
      Default: 100000
      
[警告]停机服务请先使用 kill pid 停止服务(切勿直接使用kill -9 pid等),hook回调会保证服务正常停止并清理资源
	如在使用kill pid之后[10,]秒后进程仍存活,可再使用kill -9 pid强制退出.

1) initBinlog.xml

```
<?xml version="1.0" encoding="UTF-8"?>
<xml>
 	<database dbInstanceName="ad_ins"
 		url="jdbc:mysql://127.0.0.1:3306" username="root" password="root"
 		clientId="2" fromBinlogFile="" fromBinlogOffset="" acceptEvent="0,1">
 		<db name="dsp">
 			<table subTable="ext_audit"
 				filterTimeChange="last_modify_time,create_time" />
 			<table subTable="idea_info" />
 		</db>
 	</database>
</xml>
```
1: 订阅同一数据库实例的服务机器必须保证clientId不同 <br/>
2: fromBinlogFile和fromBinlogOffset用于解决潜在问题1,用于服务B远落后与其他服务机器时,直接
   指定binlog订阅开始的文件和下标再提供服务<br/>
3: filterTimeChange是过滤掉无关字段,即只是这些字段的数据发生变化,不下发binlog事件<br/>
4: acceptEvent:用于指定当前实例上订阅的关注的下发事件,ChangeType的数值(多服务端需保证配置一致)<br/>

2) binlogAkkaServer.conf

```
akka {
  loglevel = info
  actor {
    provider = cluster
    serializers {
      jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
      jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
      proto = "akka.remote.serialization.ProtobufSerializer"
      syncEvent = "com.Rezar.dbSub.base.event.SyncEventDataSerializer"
    }
    serialization-bindings {
      "com.Rezar.dbSub.base.event.SyncEvent" = syncEvent
      "com.Rezar.dbSub.utils.CborSerializable" = jackson-cbor
    }
  }
  remote {
    artery {
      canonical.hostname = "127.0.0.1"
      canonical.port = 42767
    }
  }
  cluster {
    allow-weakly-up-members=off
    seed-nodes = [
      "akka://binlogSyncSystem@127.0.0.1:42767"]
  }
}
```

1: canonical.hostname 实际使用中请替换为内网或公网IP<br/>
2: 如果种子机器两台以上或者同一机器多端口,请先保证种子节点中排第一个服务优先启动,否则集群无法建立<br/>


#### 客户端使用说明

1) 客户端处理变更事件的业务类实现ChangeDataHandler<T> 接口
    e.g. 
```
@EventHandlerAnnot(dbInstance = "ad_ins", db = "dsp", tableName = "ext_audit", changeType={ChangeType.UPDATE}, filter = "")
public class IdeaExtAuditChangeDataHandler implements ChangeDataHandler<TestExtAudit> {

	@Getter
	private String curOffset;

	@Override
	public boolean onEvent(TestExtAudit oldData, TestExtAudit newData, String seqId, long timestamp,
			ChangeType changeType) {
		log.info("eventMsgId:{} changeType:{}", seqId, changeType);
		curOffset = seqId;
		return false;
	}

}
```
注意:dbInstance中不能包含(:)符号


其中:<br/>
1: dbInstance对应initBinlog.xml中的实例名<br/>
2: db/table 数据库实例上的库表名称<br/>
3: filter:支持数据过滤 如:filter= "where new.zmAuditStatus=0"
        即该业务处理器只处理更新事件中满足 变更后的数据中 zmAuditStatus = 0 的变更事件<br/>

2) 客户端配置akkaCluster文件(默认为binlogAkkaClient.conf)

```
akka {
  loglevel = info
  actor {
    provider = cluster
    serializers {
      jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
      jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
      proto = "akka.remote.serialization.ProtobufSerializer"
      syncEvent = "com.Rezar.dbSub.base.event.SyncEventDataSerializer"
    }
    serialization-bindings {
      "com.Rezar.dbSub.base.event.SyncEvent" = syncEvent
      "com.Rezar.dbSub.utils.CborSerializable" = jackson-cbor
    }
  }
  remote {
    artery {
      canonical.port = 0
    }
  }
  cluster {
    allow-weakly-up-members=off
    seed-nodes = [
      "akka://binlogSyncSystem@127.0.0.1:42767"
      ]
  }
}
}

```

3) 客户端启动<br/>
    
```
	1: spring : 将ClientInitWithSpring类注册到容器中
    	
    	<bean class="com.Rezar.dbSub.client.init.ClientInitWithSpring">
			<constructor-arg name="akkaConfig" value="" />
			<constructor-arg name="serverName" value="serverName" />
			<constructor-arg name="forceToAll" value="CONTINUE" />
		</bean>
		
		1) akkaConfig用于指定akka集群的配置
		2) serverName对应需要订阅binlog事件的服务名称用于日志区分
		3) forceToAll用于指定全部数据表的offset模式
```

```
    2: 非spring : 构建ClientInit并registerChangeDataHandler(xxxx)或registerSubTableInfo(xxx)
    
	    客户端订阅事件可以指定三种offset模式<br/>
	    1: CONTINUE 从服务端最新的seqId开始同步,即从连接到服务端后的位置开始同步事件,不管历史数据
	    2: LAST_POS 从客户端最后处理的seqId的下一个事件开始同步,服务端会接收客户端ack消息记录下客户端最后处理成功的数据
	    3: seqId 	从指定seqId的下一条开始同步事件
	    
	    系统默认为LAST_POS,如果需要指定模式,可对单表订阅配置一个SubTableInfo对象,offset="CONTINUE/seqId"
``` 
#### 系统说明
 
1) seqId<br/>
	标识事件的序列ID,格式为[binlog文件数字后缀_binlog文件位移_批量index]<br/>
	服务端从指定binlog文件和位移启动时,分别对应seqId的(binlog文件数字后缀)和(binlog文件位移)<br/>
```
	ps:执行 show binary logs;
	会显示 log_name 列,其中的文件名为类似:mysql-bin.000005这样,[binlog文件数字后缀]就是后面的5数字后缀,
	根据seqId反推出对应的binlog文件,需要补充前缀:mysql-bin.00000,最终得到 mysql-bin.000005 
```	
2) 客户端会在baseDir/clientInfo目录下面记录单表处理完成的seqId,用于按照LAST_POS重连服务端


3) 数据清理<br/>
	服务端会在单表数据超过一定数量(maxStore)之后清理超出数据,maxStore可在启动的时候指定-maxStore设置,需要注意的是:
	如果offset模式为LAST_POS,存在该表数据被清理后无法再次从指定的位置同步事件,尽量保证不要让客户端与服务端断连过久.
	
4) 客户端代码打包
	项目使用springboot,正常打包后是服务端可运行的jar包,如果需要打客户端使用的普通jar包,使用命令行
	mvn clean package -DskipTests=true -D spring-boot.repackage.skip=true
	
  
 #### 潜在问题
  
  1: [已解决]集群机器启动时间相差过大,会导致之前由服务A提供服务的客户端在服务A宕机重连服务B之后,带上了
  	 一个服务B不存在的lastReadSeqId.
  	 
  	 即服务B在服务A运行蛮久时间之后再加入集群中,服务B的binlog文件订阅的下标落后于服务A,由服务A
  	 运行产生的seqId下发给客户端之后,客户端又在服务A宕机后重连服务B带上给服务B,但这个lastSeqId
  	 在服务B上并未缓存对应的事件,导致事件丢失,如果客户端的重连策略是(CONTINUE),则无影响,若为FROM_OFFSET
  	 或者LAST_CONTINUE,则会出现事件丢失的情况
  	 
  	 解决:通过服务启动服务时指定binlog文件和下标来同步历史事件,以和其他服务趋于同步.

