#!/bin/python

import re

class Matcher:

    @staticmethod
    def try_to_match(line):
        pass

class RM_att_matcher(Matcher):
    ##                                                                          appid     attid       
    rm_att = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+) appattempt_(\d+)_(\d+)_(\d+) State change from (\w+) to (\w+) on event = (\S+)')

    @staticmethod
    def try_to_match(line,host):
        match =RM_att_matcher.rm_att.match(line)
        if match:
            groups=match.groups()
            time =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            #time =groups[2],":",groups[3],":",groups[4]
            app  =groups[6]+"_"+groups[7]
            ##used to count sometimes
            attid=int(groups[8])
            new  =groups[10]
            eve  =groups[11]
            event=Event(time,"RM_ATT",attid,new,eve,host)
            return app,event
        else:
            return None,None
                    
class RM_app_matcher(Matcher):

    ##match state transfer in rm RMAppImpl                                        appid 
    rm_app = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+) application_(\d+)_(\d+) State change from (\w+) to (\w+) on event = (\S+)')

    @staticmethod
    def try_to_match(line,host):
        match=RM_app_matcher.rm_app.match(line)
        if match:
            groups=match.groups()
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            #time  =groups[2],":",groups[3],":",groups[4]
            app   =groups[6]+"_"+groups[7]
            appid =int(app.split("_")[1])
            new   =groups[9]
            eve   =groups[10]
            event =Event(time,"RM_APP",appid,new,eve,host)
            return app,event
        else:
            return None,None
        
class RM_con_matcher(Matcher):
   
    ##match state transfer in rm RMContainerImpl                                appid          conid  
    rm_con = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+) container_(\d+)_(\d+)_(\d+)_(\d+) Container Transitioned from (\w+) to (\w+) on event = (\S+)')
 
    @staticmethod
    def try_to_match(line,host): 
        match=RM_con_matcher.rm_con.match(line)
        if match:
            groups=match.groups()
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            app   =groups[6]+"_"+groups[7]
            conid =int(groups[9])
            new   =groups[11]
            eve   =groups[12]
            event =Event(time,"RM_CON",conid,new,eve,host)
            return app,event
        else:
            return None,None
            
class NM_con_matcher(Matcher):

    ##match state transfer in nm
    nm_con = re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+) Container container_(\d+)_(\d+)_(\d+)_(\d+) transitioned from (\w+) to (\w+) on event = (\S+)')

    @staticmethod
    def try_to_match(line,host):
        match = NM_con_matcher.nm_con.match(line)
        if match:
            groups=match.groups()
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            app   =groups[6]+"_"+groups[7]
            conid =int(groups[9]) 
            new   =groups[11]
            eve   =groups[12]
            event =Event(time,"NM_CON",conid,new,eve,host)
            return app,event
        else:
            return None,None

"""
For spark driver logs, currently we only logs two events
(1) First log message to record the container launched time
(2) The RM-app registering event

"""
class SPARK_master_matcher(Matcher):

    ##first log message in driver
    spark_master_ini=re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (.+)TERM')
    ##when app master reigsters with RM
    spark_master_reg=re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO YarnRMClient: Registering(.+)')
    ##match log messae in spark driver stderr
    @staticmethod
    def try_to_match(line,host):
        match_ini=SPARK_master_matcher.spark_master_ini.match(line)
        if match_ini:
            groups=match_ini.groups()
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"SPARK_DRIVER",1,"INIT","INIT",host)
            return None,event

        match_reg=SPARK_master_matcher.spark_master_reg.match(line)
        if match_reg:
            groups=match_reg.groups()
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"SPARK_DRIVER",1,"REG","REG",host)
            return None,event
        
        return None,None

"""
For spark executor logs, currently we only logs two events
(1) First log message to record the task launched time
(2) The application master registered time

"""
class SPARK_slave_matcher(Matcher):
    
    ##first log message in executor
    spark_slave_dae=re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+).*Started daemon.*(\d+)@(\S+)')
    ##log message when executor get assigned first task
    spark_slave_ass=re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+) INFO (\S+) Got assigned task (\d+)')

    ## match log message in spark executor stderr
    ## container: container-id in which this executor is running
    @staticmethod
    def try_to_match(line,host,container):
        match_dae=SPARK_slave_matcher.spark_slave_dae.match(line)
        if match_dae:
            groups=match_dae.groups()
            #print groups
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            host  =groups[6]
            event =Event(time,"SPARK_EXECUTOR",container,"INIT","INIT",host)
            return None,event

        match_ass=SPARK_slave_matcher.spark_slave_ass.match(line)
        if match_ass:
            #print "match 4"
            groups=match_ass.groups()
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"SPARK_EXECUTOR",container,"ASS","ASS",host)
            return None,event
        return None,None


"""
For mapreduce master log, currently we only logs the first message
"""
class MAPREDUCE_master_matcher(Matcher):
    ##first log message
    mapreduce_master_dae=re.compile(r'(\S+) (\d+):(\d+):(\d+),(\d+).*Created AppMaster.*')

    @staticmethod
    def try_to_match(line,host,container):
        match_dae=MAPREDUCE_master_matcher.mapreduce_master_dae.match(line)
        if match_dae:
            groups=match_dae.groups()
            #print groups
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"MAPREDUCE_MASTER",container,"INIT","INIT",host)
            return None,event

        return None,None



"""
For mapreduce executor log, currently we only logs the first message
"""
class MAPREDUCE_slave_matcher(Matcher):
    ##first log message
    mapreduce_slave_dae=re.compiler(r'(\S+) (\d+):(\d+):(\d+),(\d+).*Updating Configuration.*')

    ##decide task type
    mapreduce_salve_map=re.compiler(r'(\S+) (\d+):(\d+):(\d+),(\d+).*MapTask.*')

    ##decide task type
    mapreduce_salve_reduce=re.compiler(r'(\S+) (\d+):(\d+):(\d+),(\d+).*ReduceTask.*')


    @staticmethod
    def try_to_match(line,host,container):
        match_dae=MAPREDUCE_slave_matcher.mapreduce_lave_dae.match(line)
        if match_dae:
            groups=match_dae.groups()
            #print groups
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"MAPREDUCE_SLAVE",container,"INIT","INIT",host)
            return None,event

        match_map=MAPREDUCE_slave_matcher.mapreduce_lave_map.match(line)
        if match_map:
            groups=match_map.groups()
            #print groups
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"MAPREDUCE_SLAVE",container,"ASS","MAP",host)
            return None,event

        match_reduce=MAPREDUCE_slave_matcher.mapreduce_lave_reduce.match(line)
        if match_reduce:
            groups=match_reduce.groups()
            #print groups
            time  =int(groups[1])*3600*1000+int(groups[2])*60*1000+int(groups[3])*1000+int(groups[4])
            event =Event(time,"MAPREDUCE_SLAVE",container,"ASS","REDUCE",host)
            return None,event

        return None,None






class Event:

                
    def __init__(self,time,source,id,state,eve,host):
        """
           time: time stamp, resolution is ms
           source: source of this event(e.g., rm_att)
           id: att id or container id
           state: state after the transformation
           eve:on what event trigers this transform, if it has one
           host: host of where this log is from
        """
        self.time  = time
        self.source= source
        self.id    = id
        self.state = state
        self.eve   = eve
        self.host  = host
    
    def __str__(self):
        return str(self.time)+" "+self.source+" "+str(self.id)+" "+self.state+" "+self.eve+" "+self.host

