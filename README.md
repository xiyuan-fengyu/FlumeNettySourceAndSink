## FlumeNettySourceAndSink

[TOC]

### 简介
通过 com.xiyuan.flume.sink.NettyServerSink 和 com.xiyuan.flume.source.NettyClientSource， 将公网服务器的数据采集到内网服务器

### 使用
将 release/flume-netty-source-sink-1.0.0.jar 放到 $FLUME_HOME/lib 目录下

### 配置示例
公网服务器 flume 配置  
```
# 定义agent名， source、channel、sink的名称
producer.sources = r1
producer.channels = c1
producer.sinks = k1


# 具体定义source
producer.sources.r1.type = TAILDIR
producer.sources.r1.positionFile = /usr/env/apache-flume-1.8.0/logs/taildir_position.json
producer.sources.r1.filegroups = f1
producer.sources.r1.filegroups.f1 = /user/local/nginx/logs/.*log
producer.sources.r1.fileHeader = true
producer.sources.r1.deserializer.maxLineLength = 10240
producer.sources.r1.channels = c1


# 具体定义channel
producer.channels.c1.type = memory
producer.channels.c1.capacity = 10000
producer.channels.c1.transactionCapacity = 128


#具体定义sink
producer.sinks.k1.type =com.xiyuan.flume.sink.NettyServerSink
#producer.sinks.k1.transactionTime=5000
producer.sinks.k1.host = 0.0.0.0
producer.sinks.k1.port = 9090
#producer.sinks.k1.users = user_default,user_test
producer.sinks.k1.batchSize = 32
producer.sinks.k1.channel = c1
```

内网服务器 flume 配置
```
# 定义agent名， source、channel、sink的名称
consumer.sources = r1
consumer.channels = c1
consumer.sinks = k1


# 具体定义source
consumer.sources.r1.type =com.xiyuan.flume.source.NettyClientSource
consumer.sources.r1.host = 公网服务器IP
consumer.sources.r1.port = 9090
#consumer.sources.r1.user = user_default
consumer.sources.r1.deserializer.maxLineLength = 10240
consumer.sources.r1.channels = c1


# 具体定义channel
consumer.channels.c1.type = memory
consumer.channels.c1.capacity = 10000
consumer.channels.c1.transactionCapacity = 1000


#具体定义sink
consumer.sinks.k1.type = logger
#consumer.sinks.k1.type = org.apache.spark.streaming.flume.sink.SparkSink
#consumer.sinks.k1.hostname = 0.0.0.0
#consumer.sinks.k1.port = 9090
consumer.sinks.k1.channel = c1
```  

