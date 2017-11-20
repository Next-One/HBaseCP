hbase connection pool:
configuration:
zk host ip
hbase.zookeeper.quorum=s1,s2,s3
Initialize connection num
hbase.init.connection=2
pool max connection num
hbase.max.connection=10
pool max idel connection num
hbase.maxIdel.connection=5
hbase username
hbase.username=wmx
How many seconds will the idle connection be killed
hbase.checkIdelConnection.count=30