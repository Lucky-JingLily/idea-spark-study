## 代码说明

**注意必须使用Python 3以上版本**

├── bin
  │   └── control
  ├── build.sh
├── config.py
├── etc
  │   ├── sinks.cfg
│   └── zookeeper.cfg
├─modules
  │   ├── __init__.py
│   └── zookeeper.py
├── monitor.py
├── README.md
└── sinks
  ├── __init__.py
├── hawkeye.py
└── influxdb.py


### modules

modules目录存放监控模块，监控模块需要提供monitor方法，该方法接受etc下相应配置文件的参数传入，可参考zookeeper.py

### etc

etc目录存放配置文件，需要与modules下相应的模块文件名相同，以cfg为扩展名，其中sections为cluster，子配置项会以**keywords的方式传递给modules下相应模块的monitor方法，参考zookeeper.cfg

### sinks

该目录下配置监控数据的sink，目前提供hawkeye和influxdb两个模块



## 使用说明

./monitor.py -h
usage: monitor.py [-h] [-s SERVICE] [-c CLUSTER] [-i SINK]

optional arguments:
  -h, --help            show this help message and exit
-s SERVICE, --service SERVICE
service
-c CLUSTER, --cluster CLUSTER
cluster, 以~开头为正则表达试
-t TIMEOUT, --timeout TIMEOUT
timeout, 超时设定(s), 默认: 0
-i SINK, --sink SINK  snik
  snik, 默认: hawkeye

./monitor.py                                                      #采集所有配置中的监控数据以hawkeye格式输出
  ./monitor.py --service zookeeper --cluster hb-log --sink hawkeye  #采集zookeeper hb-log的监控数据以hawkeye格式输出
  ./monitor.py --service zookeeper --cluster hb-log --sink influxdb #采集zookeeper hb-log的监控数据输出到influxdb
  ./monitor.py --service zookeeper --cluster ~^hb --sink influxdb   #采集zookeeper 以hb所有集群的监控数据输出到influxdb

### 线上使用

/export/Packages/metrics/latest/monitor-thread.py -t 10 -s hbase -i hawkeye influxdb prometheus
  /export/Packages/metrics/latest/monitor-thread.py -t 10 -s elasticsearch -i hawkeye influxdb
/export/Packages/metrics/latest/monitor-thread.py -s yarn -i hawkeye influxdb prometheus
  /export/Packages/metrics/latest/monitor-thread.py -s hdfs -i hawkeye influxdb prometheus
  /export/Packages/metrics/latest/monitor-thread.py -s zookeeper -i influxdb hawkeye prometheus
  /export/Packages/metrics/latest/monitor-thread.py -s mysql
  /export/Packages/metrics/latest/kafka.sh