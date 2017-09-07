#!/bin/python
import os

"""
(1) mkdir logs
(2) read HADOOP_HOME/etc/hadoop/slaves(workers)
(3) copy each slaves logs from slave:$HADOOP/logs to ./logs
(4) feed logs to analyze main thread
"""

def copy_logs():
    ##(1)
    os.mkdir("./logs")
    ##(2)
    hadoop_home=os.environ['$HADOOP_HOME']
    if hadoop_home is None:
        raise Exception('env errors',"$HADOOP_HOME is not set")
    ##for hadoop 3.0x
    hadoop_workers=hadoop_home+"/etc/hadoop/workers"
    hadoop_logs   =hadoop_home+"/logs"
    hadoop_workers_f=open(hadoop_workers,"r")
    for line in hadoop_workers.readlines():
        host=line.strip()
        scp_command="scp "+host+":"+hadoop_logs+"    "+"./logs/"+host+"-logs"
        ##for debug
        print scp_command
        os.system(scp_command)
    return "/logs"


def delete_logs():
    os.rmdir("./logs")
    

def get_cdf(datas):
    datas = sorted(datas)
    datas = np.array(datas)
    count,bv=np.histogram(datas,bins=100)
    cdf=np.cumsum(count)
    cdf=cdf/float(count.sum())
    return bv[1:],cdf


