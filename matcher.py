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
            if conid == 8:
                print line
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



