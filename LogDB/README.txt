0.开发环境采用cloudera cdh4.1.2

1.rowkey:IP(long)+TS(long)+RANDOM(int)+URL(长度不固定)
其中，IP:将61.160.224.140格式的IP转换成long存储；TS:使用毫秒表示,并存储Long.MAX_VALUE-ts后的值,使得记录顺序按时间由新到旧；RANDOM:随机数,保证rowkey唯一；把长度不固定的URL放在rowkey最后。

2.拷贝自己的jar到hbase的lib下：cp ./logdb.jar /usr/lib/hbase/lib/

3.运行代码中LogTable.createTable()创建HTable,后续mapreduce实现中使用了HFileOutputFormat.configureIncrementalLoad

4.运行job生成hfile：hbase org.apache.hadoop.hbase.tool.Driver /log /logdb

5.将hfile装载到hbase集群的LogTable表中：hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs:///logdb LogTable